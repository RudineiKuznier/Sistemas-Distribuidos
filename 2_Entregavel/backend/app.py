import os
import csv
import json
import uuid
import queue
import threading
import pika
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from flasgger import Swagger
import resend
from dotenv import load_dotenv
from security import verificar_evento, assinar_evento

load_dotenv()
resend.api_key = os.getenv("RESEND_API_KEY")

app = Flask(__name__)
CORS(app) 

def carregar_produtos():
    caminho_csv = os.path.join(os.path.dirname(__file__), "database.csv")
    produtos = []

    if not os.path.exists(caminho_csv):
        print(f"[ERRO] Ficheiro {caminho_csv} não encontrado.")
        return produtos

    with open(caminho_csv, mode='r', encoding='utf-8-sig') as arquivo:
        leitor = csv.DictReader(arquivo)
        for linha in leitor:
            produtos.append({
                "id": linha["id"],
                "name": linha["name"],
                "price": float(linha["price"]),     
                "category": linha["category"],
                "stock": int(linha["stock"])     
            })
    return produtos

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

NOME_PRODUTOR = "ms_principal"
CAMINHO_CHAVE_PRIVADA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    f"{NOME_PRODUTOR}.pem"
)
CAMINHO_CHAVES_PUBLICAS = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "chaves_publicas"
)

def notificar_clientes_sse(id_pedido, dados_evento):
    """Envia atualizações via SSE para as conexões ativas do frontend."""
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

        envelope = assinar_evento(dados_evento, NOME_PRODUTOR, CAMINHO_CHAVE_PRIVADA)

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
    """Worker em background que consome respostas dos microsserviços de Estoque e Pagamento."""
    while True:
        try:
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

                    mapeamento_status = {
                        "pedido.estoque_ok": "Estoque Confirmado",
                        "pagamento.aprovado": "Pagamento Aprovado",
                        "pagamento.recusado": "Pagamento Recusado",
                        "pedido.enviado": "Pedido Enviado",
                        "estoque.indisponivel": "Estoque Indisponível"
                    }

                    status_traduzido = mapeamento_status.get(rk, "STATUS_DESCONHECIDO")

                    if id_pedido in PEDIDOS:
                        PEDIDOS[id_pedido]["status"] = status_traduzido

                    notificar_clientes_sse(id_pedido, {
                        "orderId": id_pedido,
                        "status": status_traduzido,
                        "routingKey": rk,
                        "motivo": dados.get("motivo")
                    })

                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as ex:
                    print(f"[Error callback] {ex}")
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue="ms_fila_principal", on_message_callback=callback, auto_ack=False)
            channel.start_consuming()
        except Exception as e:
            print(f"[RabbitMQ Consumer Offline] Reconectando em 5s... Erro: {e}")
            threading.Event().wait(5)

threading.Thread(target=consumidor_rabbitmq_worker, daemon=True).start() #mensagens Rabbit segundo plano

@app.route('/api/products', methods=['GET'])
def get_products():
    """
    Obter catálogo de produtos
    ---
    tags:
      - Catálogo e Pedidos
    responses:
      200:
        description: Retorna a lista de produtos do catálogo
        schema:
          type: array
          items:
            type: object
            properties:
              id:
                type: string
                example: "camisa"
              name:
                type: string
                example: "Camisa Polo"
              price:
                type: number
                example: 89.90
              category:
                type: string
                example: "Vestuário"
              stock:
                type: integer
                example: 100
    """
    #return jsonify([p for p in carregar_produtos() if p["stock"] > 0]), 200
    return jsonify(carregar_produtos()), 200


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

    frontend_origin = request.headers.get("Origin", "http://localhost:5173")
    payment_url = f"{frontend_origin}/pagamento?orderId={order_id}"

    return jsonify({
        "orderId": order_id,
        "paymentUrl": payment_url
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

        status_atual = PEDIDOS.get(order_id, {}).get("status", "AGUARDANDO")
        yield f"data: {json.dumps({'orderId': order_id, 'status': status_atual})}\n\n"

        try:
            while True:
                data = q.get()
                yield f"data: {json.dumps(data)}\n\n"
        except GeneratorExit:
            sse_subscribers[order_id].remove(q)

    return Response(event_stream(), mimetype="text/event-stream")


@app.route('/api/payments/<order_id>/result', methods=['POST'])
def handle_payment_result(order_id):
    """
    Processar resultado vindo do Mock de Pagamento do Frontend
    ---
    tags:
      - Pagamento
    parameters:
      - name: order_id
        in: path
        type: string
        required: true
        description: ID do pedido
      - in: body
        name: body
        required: true
        schema:
          type: object
          required:
            - result
          properties:
            result:
              type: string
              enum: ["APROVADO", "RECUSADO"]
              example: "APROVADO"
    responses:
      200:
        description: Resultado publicado no RabbitMQ com sucesso
    """
    data = request.get_json() or {}
    resultado = data.get('result')

    payload = {
        "id_pedido": order_id,
        "resultado": resultado
    }

    enviar_evento_rabbitmq("pagamento.resultado", payload)

    return jsonify({"success": True}), 200


@app.route('/api/promotions/subscriptions', methods=['POST'])
def subscribe_promotions():
    """
    Inscrição de promoções via e-mail (Resend)
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
        description: Inscrição confirmada e e-mail enviado via Resend
      400:
        description: E-mail não informado
      500:
        description: Erro ao disparar o e-mail via Resend
    """
    data = request.get_json() or {}
    email = data.get('email')
    categories = data.get('categories', [])

    if not email:
        return jsonify({"error": "E-mail é obrigatório"}), 400

    try:
        if os.getenv("RESEND_API_KEY"):
            resend.Emails.send({
                "from": "onboarding@resend.dev",
                "to": email,
                "subject": "Inscrição de Promoções Confirmada!",
                "html": f"<p>Você se inscreveu para receber ofertas nas categorias: <strong>{', '.join(categories) if categories else 'Geral'}</strong></p>"
            })
        return jsonify({"message": "Inscrição realizada com sucesso!"}), 200
    except Exception as e:
        print(f"[Resend Error] {e}")
        return jsonify({"error": "Falha ao enviar e-mail"}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)