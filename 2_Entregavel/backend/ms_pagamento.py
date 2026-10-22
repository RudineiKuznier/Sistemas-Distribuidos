import json
import os
import secrets
import sqlite3
import threading
from contextlib import closing
from urllib.parse import quote

import pika
import requests
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
URL_MOCK_PAGAMENTO = "http://localhost:5003/api/charges"

def conectar_banco():
    return sqlite3.connect(CAMINHO_BANCO, timeout=5)

def inicializar_banco():
    with closing(conectar_banco()) as connection:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS pagamentos (
                order_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                checkout_url TEXT,
                webhook_token TEXT
            )
        """)
        colunas = {
            coluna[1]
            for coluna in connection.execute("PRAGMA table_info(pagamentos)")
        }
        if "checkout_url" not in colunas:
            connection.execute("ALTER TABLE pagamentos ADD COLUMN checkout_url TEXT")
        if "webhook_token" not in colunas:
            connection.execute("ALTER TABLE pagamentos ADD COLUMN webhook_token TEXT")
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


def criar_cobranca(id_pedido):
    with PAGAMENTOS_LOCK:
        with closing(conectar_banco()) as connection:
            pagamento = connection.execute(
                "SELECT status, checkout_url, webhook_token FROM pagamentos WHERE order_id = ?",
                (id_pedido,)
            ).fetchone()

            if pagamento is None or pagamento[0] != "PENDENTE":
                return

            checkout_url, webhook_token = pagamento[1], pagamento[2]
            if checkout_url is None:
                webhook_token = secrets.token_urlsafe(32)
                webhook_url = (
                    f"http://localhost:5002/api/payments/{id_pedido}/webhook"
                    f"?token={quote(webhook_token)}"
                )
                response = requests.post(
                    URL_MOCK_PAGAMENTO,
                    json={"order_id": id_pedido, "webhook_url": webhook_url},
                    timeout=5
                )
                response.raise_for_status()
                checkout_url = response.json().get("checkout_url")
                if not checkout_url:
                    raise ValueError("O Mock de Pagamento não retornou a URL de checkout.")

                connection.execute(
                    """UPDATE pagamentos
                       SET checkout_url = ?, webhook_token = ?
                       WHERE order_id = ? AND status = 'PENDENTE'""",
                    (checkout_url, webhook_token, id_pedido)
                )
                connection.commit()

        if not enviar_evento_rabbitmq("pagamento.checkout_criado", {
            "id_pedido": id_pedido,
            "checkout_url": checkout_url,
            "status": "AGUARDANDO_PAGAMENTO"
        }):
            raise RuntimeError("Não foi possível publicar a URL de checkout.")

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
                    if not enviar_evento_rabbitmq("pagamento.pendente", {
                        "id_pedido": id_pedido,
                        "status": "PAGAMENTO_PENDENTE"
                    }):
                        raise RuntimeError("Não foi possível publicar o estado pendente.")
                    criar_cobranca(id_pedido)
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
def registrar_resultado_pagamento(order_id, resultado, token=None):
    if resultado not in ("APROVADO", "RECUSADO"):
        return {"error": "Resultado deve ser APROVADO ou RECUSADO."}, 400

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
                return {"error": "Pagamento ainda não está disponível."}, 409

            status_atual = pagamento[0]
            token_salvo = connection.execute(
                "SELECT webhook_token FROM pagamentos WHERE order_id = ?",
                (order_id,)
            ).fetchone()[0]
            if not token or not secrets.compare_digest(token, token_salvo or ""):
                connection.rollback()
                return {"error": "Webhook inválido."}, 401
            if status_atual == resultado:
                connection.rollback()
                return {"success": True, "duplicate": True}, 200
            if status_atual != "PENDENTE":
                connection.rollback()
                return {"error": f"Pagamento já finalizado como {status_atual}."}, 409

            aprovado = resultado == "APROVADO"
            routing_key = "pagamento.aprovado" if aprovado else "pagamento.recusado"
            payload = {
                "id_pedido": order_id,
                "status": f"PAGAMENTO_{resultado}",
                "motivo": None if aprovado else "Pagamento recusado na simulação"
            }

            if not enviar_evento_rabbitmq(routing_key, payload):
                connection.rollback()
                return {"error": "Não foi possível publicar o resultado do pagamento."}, 503

            connection.execute(
                "UPDATE pagamentos SET status = ? WHERE order_id = ?",
                (resultado, order_id)
            )
            connection.commit()
            return {"success": True}, 200
        finally:
            connection.close()

@app.route('/api/payments/<order_id>/webhook', methods=['POST'])
def receber_webhook_pagamento(order_id):
    resultado = (request.get_json() or {}).get("result")
    resposta, status = registrar_resultado_pagamento(
        order_id,
        resultado,
        request.args.get("token")
    )
    return jsonify(resposta), status

@app.route('/api/payments/<order_id>/cancel', methods=['POST'])
def cancelar_pagamento(order_id):
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
            if pagamento[0] == "CANCELADO":
                connection.rollback()
                return jsonify({"success": True, "duplicate": True}), 200
            if pagamento[0] != "PENDENTE":
                connection.rollback()
                return jsonify({"error": f"Pagamento já finalizado como {pagamento[0]}."}), 409

            connection.execute(
                "UPDATE pagamentos SET status = 'CANCELADO' WHERE order_id = ?",
                (order_id,)
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
