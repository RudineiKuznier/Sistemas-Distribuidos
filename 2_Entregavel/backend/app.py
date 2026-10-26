import os
import json
import uuid
import queue
import threading
import pika
import requests
from flask import Flask, jsonify, request, Response
from flasgger import Swagger
from dotenv import load_dotenv
from security import verificar_evento, assinar_evento

load_dotenv()

ESTOQUE_URL = os.getenv("ESTOQUE_URL", "http://localhost:5001")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "http://localhost:5002/webhook")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
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

CAMINHO_CHAVE_PRIVADA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "ms_principal.pem"
)

CAMINHO_CHAVES_PUBLICAS = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "chaves_publicas"
)

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

def consumidor_rabbitmq_worker():
  while True:
    connection = criar_conexao_rabbitmq()
    channel = connection.channel()
    channel.exchange_declare(exchange="eCommerce", exchange_type="direct")
    channel.queue_declare(queue="ms_fila_principal", durable=True)
    routing_keys = (
      "pagamento.criado",
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
            mapeamento_status = {
                "pedido.estoque_ok": "Estoque Confirmado",
                "pagamento.criado": "Aguardando Pagamento",
                "pagamento.aprovado": "Pagamento Aprovado",
                "pagamento.recusado": "Pagamento Recusado",
                "pedido.enviado": "Pedido Enviado",
                "estoque.indisponivel": "Estoque Indisponível"
            }
            status_traduzido = mapeamento_status.get(rk, "STATUS_DESCONHECIDO")
            if id_pedido in PEDIDOS:
                PEDIDOS[id_pedido]["status"] = status_traduzido
                if dados.get("checkoutUrl"):
                    PEDIDOS[id_pedido]["checkoutUrl"] = dados["checkoutUrl"]
            notificar_clientes_sse(id_pedido, {
                "orderId": id_pedido,
                "status": status_traduzido,
                "routingKey": rk,
                "motivo": dados.get("motivo"),
                "checkoutUrl": dados.get("checkoutUrl")
            })
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as ex:
            print(f"[Error callback] {ex}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="ms_fila_principal", on_message_callback=callback, auto_ack=False)
    channel.start_consuming()

threading.Thread(target=consumidor_rabbitmq_worker, daemon=True).start() #mensagens Rabbit segundo plano

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
                    example: "camisa"
                  quantity:
                    type: integer
                    example: 2
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
        description: Nenhum item informado
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

    payload = {
        "id_pedido": order_id,
        "produtos": produtos_formatados,
        "status": "CRIADO"
    }
    enviar_evento_rabbitmq("pedido.criado", payload)

    return jsonify({
        "orderId": order_id
    }), 201

@app.route('/api/orders/<order_id>', methods=['DELETE'])
def delete_order(order_id):
    if order_id not in PEDIDOS:
        return jsonify({"error": "Pedido não encontrado"}), 404

    enviar_evento_rabbitmq("pedido.excluido", {"id_pedido": order_id})
    PEDIDOS[order_id]["status"] = "CANCELADO"
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
      400:
        description: E-mail não informado
    """
    data = request.get_json() or {}
    email = data.get('email')
    categories = data.get('categories', [])

    if not email:
        return jsonify({"error": "E-mail é obrigatório"}), 400

    enviar_evento_rabbitmq("interesse.promocao", {
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
      400:
        description: E-mail não informado
    """
    email = (request.get_json() or {}).get('email')
    if not email:
        return jsonify({"error": "E-mail é obrigatório"}), 400

    enviar_evento_rabbitmq("interesse.promocao.cancelado", {"email": email})
    return jsonify({"message": "Interesse cancelado"}), 200


@app.route('/api/test/webhook', methods=['POST'])
def test_webhook():
    """
    Simula o Webhook do Mock de Pagamento
    ---
    tags:
      - Testes
    parameters:
      - in: header
        name: X-Webhook-Token
        type: string
        required: true
        description: WEBHOOK_SECRET
      - in: body
        name: body
        required: true
        schema:
          type: object
          required:
            - orderId
            - status
          properties:
            orderId:
              type: string
              example: "4f850d1b"
            status:
              type: string
              enum: ["APROVADO", "RECUSADO"]
              example: "APROVADO"
    responses:
      200:
        description: Webhook aceito pelo MS Pagamento
      401:
        description: Token incorreto (confira o WEBHOOK_SECRET no .env)
      502:
        description: MS Pagamento inacessível
    """
    token = request.headers.get("X-Webhook-Token", "")
    data = request.get_json() or {}
    try:
        r = requests.post(
            WEBHOOK_URL,
            json={"orderId": data.get("orderId"), "status": data.get("status")},
            headers={"X-Webhook-Token": token},
            timeout=5,
        )
        return (r.text, r.status_code, {"Content-Type": "application/json"})
    except requests.RequestException as e:
        return jsonify({"error": f"MS Pagamento inacessível: {e}"}), 502

    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)