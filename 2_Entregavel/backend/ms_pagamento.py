import json
import os
import sqlite3
import threading
from contextlib import closing

import pika
from flask import Flask, jsonify, request
from security import verificar_evento, assinar_evento

app = Flask(__name__)
PAGAMENTOS_LOCK = threading.Lock()

NOME_PRODUTOR = "ms_pagamento"
CAMINHO_BANCO = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pagamentos.db")
CAMINHO_CHAVE_PRIVADA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    f"{NOME_PRODUTOR}.pem"
)
CAMINHO_CHAVES_PUBLICAS = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "chaves_publicas"
)

def conectar_banco():
    return sqlite3.connect(CAMINHO_BANCO, timeout=5)

def inicializar_banco():
    with closing(conectar_banco()) as connection:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS pagamentos (
                order_id TEXT PRIMARY KEY,
                status TEXT NOT NULL
            )
        """)
        connection.commit()

def registrar_pagamento_pendente(id_pedido):
    with PAGAMENTOS_LOCK, closing(conectar_banco()) as connection:
        try:
            connection.execute(
                "INSERT OR IGNORE INTO pagamentos (order_id, status) VALUES (?, 'PENDENTE')",
                (id_pedido,)
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

def criar_conexao_rabbitmq():
    return pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", virtual_host="my_vhost")
    )

def enviar_evento_rabbitmq(routing_key, dados_evento):
    connection = None
    try:
        connection = criar_conexao_rabbitmq()
        channel = connection.channel()
        channel.exchange_declare(exchange="eCommerce", exchange_type="direct")

        envelope = assinar_evento(
            dados_evento,
            NOME_PRODUTOR,
            CAMINHO_CHAVE_PRIVADA
        )
        channel.basic_publish(
            exchange="eCommerce",
            routing_key=routing_key,
            body=json.dumps(envelope),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=pika.DeliveryMode.Persistent
            )
        )
        return True
    except Exception as e:
        print(f"[RabbitMQ Producer Error] {e}")
        return False
    finally:
        if connection and connection.is_open:
            connection.close()

def consumir_pedidos_estoque():
    while True:
        connection = None
        try:
            connection = criar_conexao_rabbitmq()
            channel = connection.channel()
            channel.exchange_declare(exchange="eCommerce", exchange_type="direct")
            channel.queue_declare(queue="ms_fila_pagamento", durable=True)
            channel.queue_bind(
                exchange="eCommerce",
                queue="ms_fila_pagamento",
                routing_key="pedido.estoque_ok"
            )

            def callback(ch, method, properties, body):
                try:
                    envelope = json.loads(body.decode("utf-8"))
                    if not verificar_evento(envelope, CAMINHO_CHAVES_PUBLICAS):
                        print(f"Assinatura inválida! Evento '{method.routing_key}' descartado.")
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                        return

                    id_pedido = envelope.get("payload", {}).get("id_pedido")
                    if not id_pedido:
                        print("Evento de estoque sem ID do pedido.")
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                        return

                    registrar_pagamento_pendente(id_pedido)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    print(f"[Payment Consumer Error] {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue="ms_fila_pagamento",
                on_message_callback=callback,
                auto_ack=False
            )
            print("Aguardando pedidos com estoque confirmado.")
            channel.start_consuming()
        except Exception as e:
            print(f"[RabbitMQ Consumer Offline] Reconectando em 5s... Erro: {e}")
            threading.Event().wait(5)
        finally:
            if connection and connection.is_open:
                connection.close()

# Inicia o microserviço de pagamento, incluindo a API REST e o consumidor de eventos do RabbitMQ
@app.route('/api/payments/<order_id>/result', methods=['POST'])
def registrar_resultado_pagamento(order_id):
    resultado = (request.get_json() or {}).get("result")
    if resultado not in ("APROVADO", "RECUSADO"):
        return jsonify({"error": "Resultado deve ser APROVADO ou RECUSADO."}), 400

    with PAGAMENTOS_LOCK:
        connection = conectar_banco()
        try:
            connection.execute("BEGIN IMMEDIATE")
            pagamento = connection.execute(
                "SELECT status FROM pagamentos WHERE order_id = ?",
                (order_id,)
            ).fetchone()

            if pagamento is None:
                connection.rollback()
                return jsonify({"error": "Pagamento ainda não está disponível."}), 409

            status_atual = pagamento[0]
            if status_atual == resultado:
                connection.rollback()
                return jsonify({"success": True, "duplicate": True}), 200
            if status_atual != "PENDENTE":
                connection.rollback()
                return jsonify({"error": f"Pagamento já finalizado como {status_atual}."}), 409

            aprovado = resultado == "APROVADO"
            routing_key = "pagamento.aprovado" if aprovado else "pagamento.recusado"
            payload = {
                "id_pedido": order_id,
                "status": f"PAGAMENTO_{resultado}",
                "motivo": None if aprovado else "Pagamento recusado na simulação"
            }

            if not enviar_evento_rabbitmq(routing_key, payload):
                connection.rollback()
                return jsonify({"error": "Não foi possível publicar o resultado do pagamento."}), 503

            connection.execute(
                "UPDATE pagamentos SET status = ? WHERE order_id = ?",
                (resultado, order_id)
            )
            connection.commit()
            return jsonify({"success": True}), 200
        finally:
            connection.close()

def iniciar_ms_pagamento():
    inicializar_banco()
    threading.Thread(target=consumir_pedidos_estoque, daemon=True).start()
    app.run(host="0.0.0.0", port=5002, debug=False, use_reloader=False)

if __name__ == "__main__":
    iniciar_ms_pagamento()
