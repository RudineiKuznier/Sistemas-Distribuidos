import pika
import json
import os
import threading
import requests
from flask import Flask, request
from security import verificar_evento, assinar_evento

NOME_PRODUTOR = "ms_pagamento"
BASE = os.path.dirname(os.path.abspath(__file__))
CAMINHO_CHAVE_PRIVADA = os.path.join(BASE, f"{NOME_PRODUTOR}.pem")
CAMINHO_CHAVES_PUBLICAS = os.path.join(BASE, "chaves_publicas")

MOCK_URL = os.getenv("MOCK_URL", "http://localhost:5003")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "http://localhost:5002/webhook")


def publicar(channel, routing_key, dados_evento):
    envelope = assinar_evento(dados_evento, NOME_PRODUTOR, CAMINHO_CHAVE_PRIVADA)
    channel.basic_publish(
        exchange='eCommerce',
        routing_key=routing_key,
        body=json.dumps(envelope),
        properties=pika.BasicProperties(
            content_type='application/json',
            delivery_mode=pika.DeliveryMode.Persistent
        )
    )
    print(f"    Enviado para eCommerce: {routing_key}")

api = Flask(__name__)


@api.post('/webhook')
def webhook():
    d = request.get_json() or {}
    id_pedido, status = d.get("orderId"), d.get("status")
    if not id_pedido or status not in ("APROVADO", "RECUSADO"):
        return {"error": "payload inválido"}, 400

    if status == "APROVADO":
        rk = 'pagamento.aprovado'
        payload = {"id_pedido": id_pedido, "status": "PAGAMENTO_APROVADO"}
    else:
        rk = 'pagamento.recusado'
        payload = {"id_pedido": id_pedido, "status": "PAGAMENTO_RECUSADO",
                   "motivo": "Pagamento recusado"}

    # conexão própria: esta thread não pode usar a do consumidor
    conn = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', virtual_host='my_vhost'))
    try:
        ch = conn.channel()
        ch.exchange_declare(exchange='eCommerce', exchange_type='direct')
        publicar(ch, rk, payload)
    finally:
        conn.close()
    return {"ok": True}, 200

def criar_cobranca(id_pedido):
    r = requests.post(f"{MOCK_URL}/charges", json={
        "orderId": id_pedido,
        "webhookUrl": WEBHOOK_URL
    }, timeout=3)
    r.raise_for_status()
    return r.json()["checkoutUrl"]


def callback(ch, method, properties, body):
    try:
        envelope = json.loads(body.decode('utf-8'))

        if not verificar_evento(envelope, CAMINHO_CHAVES_PUBLICAS):
            print(f"    Assinatura inválida! Evento '{method.routing_key}' descartado.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        dados = envelope.get('payload', {})
        id_pedido = dados.get('id_pedido')
        print("-" * 40)
        print(f"Routing Key: {method.routing_key}")
        print(f"ID do Pedido: {id_pedido}")

        if method.routing_key == 'pedido.estoque_ok':
            checkout_url = criar_cobranca(id_pedido)
            publicar(ch, 'pagamento.criado', {
                "id_pedido": id_pedido,
                "status": "AGUARDANDO_PAGAMENTO",
                "checkoutUrl": checkout_url
            })

        print("-" * 40)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"    [Erro callback pagamento] {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)


def iniciar_ms_pagamento():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', virtual_host='my_vhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='eCommerce', exchange_type='direct')

    queue_name = 'ms_fila_pagamento'
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(exchange='eCommerce', queue=queue_name, routing_key='pedido.estoque_ok')

    print('     Aguardando eventos de pagamento. Para sair pressione CTRL+C')
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
    channel.start_consuming()


if __name__ == '__main__':
    threading.Thread(
        target=lambda: api.run(host='0.0.0.0', port=5002, use_reloader=False),
        daemon=True
    ).start()
    iniciar_ms_pagamento()