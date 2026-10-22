import json
import os
import threading

import pika
from security import verificar_evento, assinar_evento

NOME_PRODUTOR = "ms_entrega"
CAMINHO_CHAVE_PRIVADA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    f"{NOME_PRODUTOR}.pem"
)
CAMINHO_CHAVES_PUBLICAS = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "chaves_publicas"
)


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
    except Exception as erro:
        print(f"[RabbitMQ Producer Error] {erro}")
        return False
    finally:
        if connection and connection.is_open:
            connection.close()


def callback(ch, method, properties, body):
    try:
        envelope = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as erro:
        print(f"[Delivery Consumer Error] Mensagem inválida: {erro}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    if not verificar_evento(envelope, CAMINHO_CHAVES_PUBLICAS):
        print(f"Assinatura inválida! Evento '{method.routing_key}' descartado.")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    dados = envelope.get("payload")
    if not isinstance(dados, dict):
        print("Evento de pagamento aprovado sem payload válido.")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    id_pedido = dados.get("id_pedido")
    if not isinstance(id_pedido, str) or not id_pedido.strip():
        print("Evento de pagamento aprovado sem ID do pedido.")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    try:
        print(f"Nota fiscal simulada para o pedido {id_pedido}.")
        print(f"Despacho simulado para o pedido {id_pedido}.")

        if not enviar_evento_rabbitmq("pedido.enviado", {
            "id_pedido": id_pedido,
            "status": "PEDIDO_ENVIADO",
            "nota_fiscal_emitida": True
        }):
            raise RuntimeError("Não foi possível publicar o pedido enviado.")

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as erro:
        print(f"[Delivery Consumer Error] {erro}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def consumir_pagamentos_aprovados():
    while True:
        connection = None
        try:
            connection = criar_conexao_rabbitmq()
            channel = connection.channel()
            channel.exchange_declare(exchange="eCommerce", exchange_type="direct")
            channel.queue_declare(queue="ms_fila_entrega", durable=True)
            channel.queue_bind(
                exchange="eCommerce",
                queue="ms_fila_entrega",
                routing_key="pagamento.aprovado"
            )

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue="ms_fila_entrega",
                on_message_callback=callback,
                auto_ack=False
            )
            print("Aguardando pagamentos aprovados.")
            channel.start_consuming()
        except Exception as erro:
            print(f"[RabbitMQ Consumer Offline] Reconectando em 5s... Erro: {erro}")
            threading.Event().wait(5)
        finally:
            if connection and connection.is_open:
                connection.close()


def iniciar_ms_entrega():
    if not os.path.isfile(CAMINHO_CHAVE_PRIVADA):
        raise FileNotFoundError(
            f"Chave privada do MS Entrega não encontrada: {CAMINHO_CHAVE_PRIVADA}"
        )

    consumir_pagamentos_aprovados()


if __name__ == "__main__":
    iniciar_ms_entrega()
