import pika
import json
import os
import threading
import requests
from flask import Flask, request
from security import verificar_evento, assinar_evento

BASE = os.path.dirname(os.path.abspath(__file__))
CAMINHO_CHAVE_PRIVADA = os.path.join(BASE, "ms_pagamento.pem")
CAMINHO_CHAVES_PUBLICAS = os.path.join(BASE, "chaves_publicas")

MOCK_URL = os.getenv("MOCK_URL", "http://localhost:5003")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "http://localhost:5002/webhook")

CHECKOUTS = {} 

api = Flask(__name__)

def publicar(channel, routing_key, dados_evento):
    envelope = assinar_evento(dados_evento, "ms_pagamento", CAMINHO_CHAVE_PRIVADA)
    channel.basic_publish(
        exchange='eCommerce',
        routing_key=routing_key,
        body=json.dumps(envelope),
        properties=pika.BasicProperties(
            content_type='application/json',
            delivery_mode=pika.DeliveryMode.Persistent
        )
    )
    print(f"    Enviado ({routing_key}): {dados_evento.get('id_pedido')}")

@api.get('/charges/<id_pedido>')
def consultar_cobranca(id_pedido):
    url = CHECKOUTS.get(id_pedido)
    if not url:
        return {"error": "cobrança não encontrada"}, 404
    return {"checkoutUrl": url}, 200

RESOLVIDOS = set()

@api.post('/webhook')
def webhook():
    d = request.get_json() or {}
    id_pedido, status = d.get("orderId"), d.get("status")
    if not id_pedido or status not in ("APROVADO", "RECUSADO"):
        return {"error": "payload inválido"}, 400
    if id_pedido in RESOLVIDOS:
        return {"ok": True, "ignorado": True}, 200

    if status == "APROVADO":
        rk = 'pagamento.aprovado'
        payload = {"id_pedido": id_pedido, "status": "PAGAMENTO_APROVADO"}
    else:
        rk = 'pagamento.recusado'
        payload = {"id_pedido": id_pedido, "status": "PAGAMENTO_RECUSADO",
                   "motivo": "Pagamento recusado"}

    conn = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', virtual_host='my_vhost'))
    try:
        ch = conn.channel()
        ch.exchange_declare(exchange='eCommerce', exchange_type='direct')
        publicar(ch, rk, payload)
        RESOLVIDOS.add(id_pedido)
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
        CHECKOUTS[id_pedido] = criar_cobranca(id_pedido)
        
    print("-" * 40)
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