import os
import json
import uuid
import queue
import threading
import pika
import requests
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
PEDIDOS_LOCK = threading.Lock()

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

def processar_evento_pedido(routing_key, dados):
    id_pedido = dados.get("id_pedido")
    mapeamento_status = {
        "pedido.estoque_ok": "Estoque Confirmado",
        "pagamento.aprovado": "Pagamento Aprovado",
        "pagamento.recusado": "Pagamento Recusado",
        "pedido.enviado": "Pedido Enviado",
        "estoque.indisponivel": "Estoque Indisponível",
    }

    # Atualiza o status do pedido com base na routing key recebida e notifica os clientes SSE
    with PEDIDOS_LOCK:
        pedido = PEDIDOS.setdefault(id_pedido, {
            "status": "Aguardando atualizações",
            "routingKey": None,
            "estoqueConfirmado": False,
            "estoqueDevolvido": False,
        })

        if routing_key == "pedido.estoque_ok":
            pedido["status"] = mapeamento_status[routing_key]
            pedido["estoqueConfirmado"] = True
        elif routing_key == "estoque.devolvido":
            pedido["estoqueDevolvido"] = True
        else:
            pedido["status"] = mapeamento_status.get(routing_key, "STATUS_DESCONHECIDO")

        pedido["routingKey"] = routing_key
        evento_sse = {
            "orderId": id_pedido,
            "status": pedido["status"],
            "routingKey": routing_key,
            "estoqueConfirmado": pedido["estoqueConfirmado"],
            "estoqueDevolvido": pedido["estoqueDevolvido"],
            "motivo": dados.get("motivo")
        }

    notificar_clientes_sse(id_pedido, evento_sse)

def criar_conexao_rabbitmq():
    return pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", virtual_host="my_vhost")
    )

def enviar_evento_rabbitmq(routing_key, dados_evento):
    """Assina o payload e publica no exchange 'eCommerce' do RabbitMQ."""
    connection = None
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
        return True
    except Exception as e:
        print(f"[RabbitMQ Producer Error] {e}")
        return False
    finally:
        if connection and connection.is_open:
            connection.close()

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
                "estoque.devolvido",
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
                    processar_evento_pedido(method.routing_key, dados)

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
    Listar produtos disponíveis
    ---
    tags:
      - Catálogo e Pedidos
    responses:
      200:
        description: Produtos disponíveis em estoque.
        schema:
          type: array
          items:
            type: object
            properties:
              id:
                type: string
              name:
                type: string
              price:
                type: number
              category:
                type: string
              stock:
                type: integer
      502:
        description: MS Estoque indisponível.
    """
    try:
        response = requests.get("http://localhost:5001/api/products", timeout=3)
        response.raise_for_status()
        return jsonify(response.json()), 200
    except requests.RequestException as e:
        print(f"[MS Estoque Error] {e}")
        return jsonify({"error": "Não foi possível consultar o estoque"}), 502

@app.route('/api/orders', methods=['POST'])
def create_order():
    """
    Criar pedido
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
                  quantity:
                    type: integer
    responses:
      201:
        description: Pedido criado e enviado para processamento.
        schema:
          type: object
          properties:
            orderId:
              type: string
            paymentUrl:
              type: string
      400:
        description: Itens não informados.
      503:
        description: Não foi possível publicar o pedido.
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

    with PEDIDOS_LOCK:
        PEDIDOS[order_id] = {
            "status": "Pedido criado",
            "routingKey": None,
            "estoqueConfirmado": False,
            "estoqueDevolvido": False,
        }

    payload = {
        "id_pedido": order_id,
        "produtos": produtos_formatados,
        "status": "CRIADO"
    }
    if not enviar_evento_rabbitmq("pedido.criado", payload):
        with PEDIDOS_LOCK:
            PEDIDOS.pop(order_id, None)
        return jsonify({"error": "Não foi possível publicar o pedido."}), 503

    frontend_origin = request.headers.get("Origin", "http://localhost:5173")
    payment_url = f"{frontend_origin}/pagamento?orderId={order_id}"

    return jsonify({
        "orderId": order_id,
        "paymentUrl": payment_url
    }), 201

@app.route('/api/orders/<order_id>/events', methods=['GET'])
def sse_order_stream(order_id):
    """
    Acompanhar atualizações do pedido (SSE)
    ---
    tags:
      - Acompanhamento de Pedidos (SSE)
    parameters:
      - name: order_id
        in: path
        type: string
        required: true
    produces:
      - text/event-stream
    responses:
      200:
        description: Eventos JSON com status, routingKey e estado do estoque.
    """
    def event_stream():
        q = queue.Queue()
        if order_id not in sse_subscribers:
            sse_subscribers[order_id] = []
        sse_subscribers[order_id].append(q)

        pedido = PEDIDOS.get(order_id, {})
        estado_inicial = {
            "orderId": order_id,
            "status": pedido.get("status", "AGUARDANDO"),
            "routingKey": pedido.get("routingKey"),
            "estoqueConfirmado": pedido.get("estoqueConfirmado", False),
            "estoqueDevolvido": pedido.get("estoqueDevolvido", False),
        }
        yield f"data: {json.dumps(estado_inicial)}\n\n"

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
    Registrar resultado do pagamento
    ---
    tags:
      - Pagamento
    parameters:
      - name: order_id
        in: path
        type: string
        required: true
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
    responses:
      200:
        description: Resultado registrado; repetição idêntica é idempotente.
      400:
        description: Resultado inválido.
      404:
        description: Pedido não encontrado.
      409:
        description: Pagamento pendente ou finalizado com resultado oposto.
      502:
        description: MS Pagamento indisponível.
      503:
        description: Não foi possível publicar o resultado.
    """
    data = request.get_json() or {}
    resultado = data.get('result')

    if resultado not in ("APROVADO", "RECUSADO"):
        return jsonify({"error": "Resultado deve ser APROVADO ou RECUSADO."}), 400

    with PEDIDOS_LOCK:
        pedido = PEDIDOS.get(order_id)
        if pedido is None:
            return jsonify({"error": "Pedido não encontrado."}), 404

    try:
        response = requests.post(
            f"http://localhost:5002/api/payments/{order_id}/result",
            json={"result": resultado},
            timeout=5,
        )
        return jsonify(response.json()), response.status_code
    except requests.RequestException as e:
        print(f"[MS Pagamento Error] {e}")
        return jsonify({"error": "Não foi possível consultar o MS Pagamento."}), 502

@app.route('/api/promotions/subscriptions', methods=['POST'])
def subscribe_promotions():
    """
    Inscrever e-mail para promoções
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
            categories:
              type: array
              items:
                type: string
    responses:
      200:
        description: Inscrição processada.
      400:
        description: E-mail não informado.
      500:
        description: Falha ao enviar o e-mail.
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
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)