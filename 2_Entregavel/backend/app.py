import os
import json
import uuid
import queue
import time
import threading
import pika
import requests
from flask import Flask, jsonify, request, Response
from flasgger import Swagger
from security import verificar_evento, assinar_evento

PAGAMENTO_URL = os.getenv("PAGAMENTO_URL", "http://localhost:5002")
ESTOQUE_URL = os.getenv("ESTOQUE_URL", "http://localhost:5001")

app = Flask(__name__)

swagger_template = {
    "swagger": "2.0",
    "info": {
        "title": "API Gateway / MS Principal - E-Commerce",
        "description": "API REST e canal SSE para o Frontend Web, integrado ao RabbitMQ e Resend.",
        "version": "1.0.0"
    }
}

swagger = Swagger(app, template=swagger_template)
PEDIDOS = {}
sse_subscribers = {}
ESTOQUE_EVENTS = {}

CAMINHO_CHAVE_PRIVADA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "ms_principal.pem"
)

CAMINHO_CHAVES_PUBLICAS = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "chaves_publicas"
)

STATUS_FINAIS = ("Pagamento Aprovado", "Pedido Enviado", "Pedido Cancelado")

def notificar_clientes_sse(id_pedido, dados_evento):
    if id_pedido in sse_subscribers:
        for q in sse_subscribers[id_pedido]:
            q.put(dados_evento)

def criar_conexao_rabbitmq():
    return pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", virtual_host="my_vhost")
    )

def enviar_evento_rabbitmq(routing_key, dados_evento):
    try:
        connection = criar_conexao_rabbitmq()
        channel = connection.channel()
        channel.exchange_declare(exchange="eCommerce", exchange_type="direct")

        envelope = assinar_evento(dados_evento, "ms_principal", CAMINHO_CHAVE_PRIVADA)

        channel.basic_publish(
            exchange="eCommerce",
            routing_key=routing_key,
            body=json.dumps(envelope),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=pika.DeliveryMode.Persistent
            )
        )
        connection.close()
    except Exception as e:
        print(f"[RabbitMQ Producer Error] {e}")

MAPEAMENTO_STATUS = {
  "pedido.estoque_ok": "Estoque Confirmado",
  "pagamento.aprovado": "Pagamento Aprovado",
  "pagamento.recusado": "Pagamento Recusado",
  "pedido.enviado": "Pedido Enviado",
  "estoque.indisponivel": "Estoque Indisponível"
}

def consumidor_rabbitmq_worker():
  while True:
    connection = criar_conexao_rabbitmq()
    channel = connection.channel()
    channel.exchange_declare(exchange="eCommerce", exchange_type="direct")
    channel.queue_declare(queue="ms_fila_principal", durable=True)

    routing_keys = (
      "pagamento.aprovado",
      "pagamento.recusado",
      "pedido.enviado",
      "pedido.estoque_ok",
      "estoque.indisponivel",
    )
    for rk in routing_keys:
        channel.queue_bind(exchange="eCommerce", queue="ms_fila_principal", routing_key=rk)
    
    def callback(ch, method, properties, body):
        try:
            envelope = json.loads(body.decode("utf-8"))
            if not verificar_evento(envelope, CAMINHO_CHAVES_PUBLICAS):
                print(f"Assinatura inválida! Evento '{method.routing_key}' descartado.")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return
            
            dados = envelope.get("payload", {})
            id_pedido = dados.get("id_pedido")
            rk = method.routing_key

            pedido = PEDIDOS.get(id_pedido)
            if pedido and pedido["status"] == "Pedido Cancelado":
              ch.basic_ack(delivery_tag=method.delivery_tag)
              return

            status_traduzido = MAPEAMENTO_STATUS.get(rk, "STATUS_DESCONHECIDO")
            if pedido:
              pedido["status"] = status_traduzido

            notificar_clientes_sse(id_pedido, {
                "orderId": id_pedido,
                "status": status_traduzido,
                "routingKey": rk,
                "motivo": dados.get("motivo")
            })

            if rk in ("pedido.estoque_ok", "estoque.indisponivel"):
              ev = ESTOQUE_EVENTS.get(id_pedido)
              if ev:
                  ev.set()

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as ex:
            print(f"[Error callback] {ex}")
            ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="ms_fila_principal", on_message_callback=callback, auto_ack=False)
    channel.start_consuming()

threading.Thread(target=consumidor_rabbitmq_worker, daemon=True).start() #mensagens Rabbit segundo plano

#Consulta até a cobrança existir
def buscar_checkout_url(order_id, tentativas=10):
    for _ in range(tentativas):
        try:
            r = requests.get(f"{PAGAMENTO_URL}/charges/{order_id}", timeout=2)
            if r.status_code == 200:
                return r.json()["checkoutUrl"]
        except requests.RequestException:
            pass
        time.sleep(0.3)
    return None

@app.route('/api/products', methods=['GET'])
def get_products():
    """
    Listar produtos em estoque (consulta o MS Estoque via REST)
    ---
    tags:
      - Catálogo e Pedidos
    responses:
      200:
        description: Lista de produtos disponíveis
    """
    return requests.get(f"{ESTOQUE_URL}/products", timeout=3).json()


@app.route('/api/orders', methods=['POST'])
def create_order():
    """
    Criar pedido e publicar 'pedido.criado' no RabbitMQ
    ---
    tags:
      - Catálogo e Pedidos
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
          required:
            - items
          properties:
            items:
              type: array
              items:
                type: object
                properties:
                  productId:
                    type: string
                    example: "5"
                  quantity:
                    type: integer
                    example: 1
    responses:
      201:
        description: Pedido criado com sucesso. Retorna orderId e a URL do mock de pagamento.
        schema:
          type: object
          properties:
            orderId:
              type: string
              example: "p101"
            paymentUrl:
              type: string
              example: "http://localhost:5173/pagamento?orderId=p101"
      400:
        description: Requisição inválida sem itens.
    """
    data = request.get_json() or {}
    items = data.get('items', [])

    if not items:
        return jsonify({"error": "Nenhum item informado"}), 400

    order_id = str(uuid.uuid4())[:8]

    produtos_formatados = [
        {"nome": item.get("productId"), "quantidade": item.get("quantity")}
        for item in items
    ]

    PEDIDOS[order_id] = {
        "id_pedido": order_id,
        "produtos": produtos_formatados,
        "status": "CRIADO"
    }

    evento = threading.Event()
    ESTOQUE_EVENTS[order_id] = evento

    enviar_evento_rabbitmq("pedido.criado", {
        "id_pedido": order_id,
        "produtos": produtos_formatados,
        "status": "CRIADO"
    })

    evento.wait(timeout=8)
    ESTOQUE_EVENTS.pop(order_id, None)

    payment_url = None
    if PEDIDOS[order_id]["status"] == "Estoque Confirmado":
        payment_url = buscar_checkout_url(order_id)

    return jsonify({"orderId": order_id, "paymentUrl": payment_url}), 201

@app.route('/api/orders/<order_id>', methods=['DELETE'])
def delete_order(order_id):
    """
    Cancelar pedido e publicar 'pedido.excluido' no RabbitMQ
    ---
    tags:
      - Catálogo e Pedidos
    parameters:
      - name: order_id
        in: path
        type: string
        required: true
    responses:
      200:
        description: Pedido cancelado
    """

    pedido = PEDIDOS.get(order_id)
    if not pedido:
        return jsonify({"error": "Pedido não encontrado"}), 404
    if pedido["status"] in STATUS_FINAIS:
        return jsonify({"error": "Pedido não pode mais ser cancelado"}), 409

    pedido["status"] = "Pedido Cancelado"
    enviar_evento_rabbitmq("pedido.excluido", {"id_pedido": order_id})
    notificar_clientes_sse(order_id, {
        "orderId": order_id,
        "status": "Pedido Cancelado",
        "routingKey": "pedido.excluido"
    })
    return jsonify({"success": True}), 200


@app.route('/api/orders/<order_id>/events', methods=['GET'])
def sse_order_stream(order_id):
  """
    Canal de acompanhamento do pedido em tempo real (SSE)
    ---
    tags:
      - Acompanhamento de Pedidos (SSE)
    parameters:
      - name: order_id
        in: path
        type: string
        required: true
        description: ID do pedido a ser acompanhado
    produces:
      - text/event-stream
    responses:
      200:
        description: Conexão SSE estabelecida com sucesso
    """
  def event_stream():
    q = queue.Queue()
    if order_id not in sse_subscribers:
        sse_subscribers[order_id] = []
    sse_subscribers[order_id].append(q)

    pedido = PEDIDOS.get(order_id, {})
    yield f"data: {json.dumps({'orderId': order_id, 'status': pedido.get('status', 'AGUARDANDO'), 'checkoutUrl': pedido.get('checkoutUrl')})}\n\n"
   
    try:
        while True:
            data = q.get()
            yield f"data: {json.dumps(data)}\n\n"
    except GeneratorExit:
        sse_subscribers[order_id].remove(q)

  return Response(event_stream(), mimetype="text/event-stream")

@app.route('/api/promotions/subscriptions', methods=['POST'])
def subscribe_promotions():
    """
    Registrar interesse em promoções (publica 'interesse.promocao')
    ---
    tags:
      - Inscrição de Promoções
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
          required:
            - email
          properties:
            email:
              type: string
              example: "cliente@exemplo.com"
            categories:
              type: array
              items:
                type: string
              example: ["Vestuário", "Acessórios"]
    responses:
      200:
        description: Interesse registrado
    """
    data = request.get_json() or {}
    email = data.get('email')
    categories = data.get('categories', [])

    if not email:
        return jsonify({"error": "E-mail é obrigatório"}), 400

    enviar_evento_rabbitmq("interesse.promocao", {
        "acao": "inscrever",
        "email": email,
        "categorias": categories
    })
    return jsonify({"message": "Interesse registrado!"}), 200


@app.route('/api/promotions/subscriptions', methods=['DELETE'])
def unsubscribe_promotions():
    """
    Cancelar interesse em promoções
    ---
    tags:
      - Inscrição de Promoções
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
          required:
            - email
          properties:
            email:
              type: string
    responses:
      200:
        description: Interesse cancelado
    """
    email = (request.get_json() or {}).get('email')
    if not email:
        return jsonify({"error": "E-mail é obrigatório"}), 400

    enviar_evento_rabbitmq("interesse.promocao", {
        "acao": "cancelar",
        "email": email,
        "categorias": []
    })
    return jsonify({"message": "Interesse cancelado"}), 200

    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)  