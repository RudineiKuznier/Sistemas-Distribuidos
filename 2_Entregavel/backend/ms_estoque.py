import json
import os
import threading

import pika
from flask import Flask, jsonify
from sqlalchemy import Float, ForeignKey, Integer, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
from security import verificar_evento, assinar_evento

# Configuração inicial do banco de dados
PRODUTOS_INICIAIS = [
    {"id": "camisa", "name": "Camisa Polo", "price": 89.90, "category": "Vestuário", "stock": 100},
    {"id": "tenis", "name": "Tênis Esportivo", "price": 299.90, "category": "Calçados", "stock": 2},
    {"id": "calca", "name": "Calça Jeans", "price": 149.90, "category": "Vestuário", "stock": 0},
    {"id": "mochila", "name": "Mochila Escolar", "price": 120.00, "category": "Acessórios", "stock": 1},
    {"id": "casaco", "name": "Casaco de Frio", "price": 250.00, "category": "Vestuário", "stock": 9},
    {"id": "meia", "name": "Meia", "price": 19.90, "category": "Vestuário", "stock": 10},
    {"id": "bone", "name": "Boné", "price": 39.90, "category": "Acessórios", "stock": 1},
    {"id": "bermuda", "name": "Bermuda", "price": 89.90, "category": "Vestuário", "stock": 110},
    {"id": "cinto", "name": "Cinto", "price": 39.90, "category": "Acessórios", "stock": 50},
    {"id": "sapato", "name": "Sapato", "price": 199.90, "category": "Calçados", "stock": 40},
]

NOME_PRODUTOR = "ms_estoque"

CAMINHO_CHAVE_PRIVADA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    f"{NOME_PRODUTOR}.pem"
)

CAMINHO_CHAVES_PUBLICAS = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "chaves_publicas"
)

app = Flask(__name__)

# Configuração do banco de dados SQLite
CAMINHO_BANCO = os.path.join(os.path.dirname(os.path.abspath(__file__)), "estoque.db")
engine = create_engine(
    f"sqlite:///{CAMINHO_BANCO}",
    connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(bind=engine)

# Definição das classes do SQLAlchemy para o banco de dados
class Base(DeclarativeBase):
    pass

class Produto(Base):
    __tablename__ = "produtos"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String)
    price: Mapped[float] = mapped_column(Float)
    category: Mapped[str] = mapped_column(String)
    stock: Mapped[int] = mapped_column(Integer)

class Reserva(Base):
    __tablename__ = "reservas"

    order_id: Mapped[str] = mapped_column(String, primary_key=True)
    product_id: Mapped[str] = mapped_column(ForeignKey("produtos.id"), primary_key=True)
    quantity: Mapped[int] = mapped_column(Integer)

# Função para inicializar o banco de dados e popular com produtos iniciais
def inicializar_banco():
    Base.metadata.create_all(engine)

    with SessionLocal.begin() as session:
        if session.query(Produto).count() == 0:
            session.add_all(Produto(**produto) for produto in PRODUTOS_INICIAIS)

# Endpoint da API REST para listar produtos e seus estoques atuais
@app.route('/api/products', methods=['GET'])
def listar_produtos():
    """Lista os produtos e seus estoques atuais."""
    with SessionLocal() as session:
        produtos = session.query(Produto).order_by(Produto.id).all()
        return jsonify([
            {
                "id": produto.id,
                "name": produto.name,
                "price": produto.price,
                "category": produto.category,
                "stock": produto.stock
            }
            for produto in produtos
        ]), 200

# Inicia a API REST em uma thread separada
def iniciar_api_rest():
    app.run(host="0.0.0.0", port=5001, debug=False, use_reloader=False)

# Inicia o microserviço de estoque, incluindo a API REST e o consumidor de eventos do RabbitMQ
def iniciar_ms_estoque():
    inicializar_banco()
    threading.Thread(target=iniciar_api_rest, daemon=True).start()

    # Inicializa a conexão com o RabbitMQ
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', virtual_host='my_vhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='eCommerce', exchange_type='direct')

    queue_name = 'ms_fila_estoque'

    channel.queue_declare(queue=queue_name, durable=True)

    channel.queue_bind(exchange='eCommerce', queue=queue_name, routing_key='pedido.criado')
    channel.queue_bind(exchange='eCommerce', queue=queue_name, routing_key='pedido.excluido')
    channel.queue_bind(exchange='eCommerce', queue=queue_name, routing_key='pagamento.recusado')

    print(' [*] Aguardando eventos de estoque. Para sair pressione CTRL+C')

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=queue_name, on_message_callback=callback, auto_ack=False)

    channel.start_consuming()

# Envia eventos para o exchange "eCommerce" com a routing key especificada
def enviar_evento_estoque(channel, routing_key, dados_evento):
    envelope_assinado = assinar_evento(
        dados_evento,
        NOME_PRODUTOR,
        CAMINHO_CHAVE_PRIVADA
    )

    message = json.dumps(envelope_assinado)
    channel.basic_publish(
        exchange='eCommerce',
        routing_key=routing_key,
        body=message,
        properties=pika.BasicProperties(
            content_type='application/json',
            delivery_mode=pika.DeliveryMode.Persistent
        )
    )
    print(f"    Enviado para eCommerce {routing_key}: {message}")

# Processa o evento "pedido.criado" verificando a disponibilidade de estoque e reservando os produtos
def processar_pedido_criado(ch, id_pedido, itens):
    quantidades = {}
    for item in itens:
        produto = item.get("nome") or item.get("produto")
        quantidade = item.get("quantidade", 1)
        quantidades[produto] = quantidades.get(produto, 0) + quantidade

    motivo = None
    reserva_existente = False

    # Verifica se já existe uma reserva para o pedido e se há estoque suficiente para os produtos solicitados
    with SessionLocal.begin() as session:
        reservas = session.query(Reserva).filter_by(order_id=id_pedido).all()
        if reservas:
            reserva_existente = True
        elif not quantidades:
            motivo = "Nenhum produto informado"
        else:
            for produto_id, quantidade in quantidades.items():
                produto = session.get(Produto, produto_id)
                if not produto or quantidade <= 0 or produto.stock < quantidade:
                    motivo = f"Produto {produto_id} sem estoque suficiente"
                    break

            if motivo is None:
                for produto_id, quantidade in quantidades.items():
                    produto = session.get(Produto, produto_id)
                    produto.stock -= quantidade
                    session.add(Reserva(
                        order_id=id_pedido,
                        product_id=produto_id,
                        quantity=quantidade
                    ))
    # Envia o evento de estoque indisponível se houver algum motivo, caso contrário envia o evento de estoque reservado com sucesso
    if motivo:
        print(f"    Status: Pedido {id_pedido} -> ESTOQUE INDISPONÍVEL ({motivo})")
        enviar_evento_estoque(ch, 'estoque.indisponivel', {
            "id_pedido": id_pedido,
            "status": "ESTOQUE_INDISPONIVEL",
            "motivo": motivo
        })
        return
    
    if reserva_existente:
        print(f"    Pedido {id_pedido} já possui reserva.")
    else:
        print(f"    Status: Pedido {id_pedido} -> ESTOQUE RESERVADO COM SUCESSO")

    enviar_evento_estoque(ch, 'pedido.estoque_ok', {
        "id_pedido": id_pedido,
        "produtos": [
            {"nome": produto_id, "quantidade": quantidade}
            for produto_id, quantidade in quantidades.items()
        ],
        "status": "ESTOQUE_OK"
    })

# Processa o evento "pedido.excluido" e devolve os produtos reservados ao estoque
def processar_pedido_excluido(ch, id_pedido):
    devolver_reserva(id_pedido)
    enviar_evento_estoque(ch, 'estoque.devolvido', {
        "id_pedido": id_pedido,
        "status": "ESTOQUE_DEVOLVIDO",
        "motivo": "PEDIDO_CANCELADO"
    })

# Devolve ao estoque os produtos reservados para o pedido
def devolver_reserva(id_pedido):
    with SessionLocal.begin() as session:
        reservas = session.query(Reserva).filter_by(order_id=id_pedido).all()

        for reserva in reservas:
            produto = session.get(Produto, reserva.product_id)
            if produto:
                produto.stock += reserva.quantity
            session.delete(reserva)

    if reservas:
        print(f"    Status: Pedido {id_pedido} -> PRODUTOS DEVOLVIDOS AO ESTOQUE")
    else:
        print(f"    Status: Pedido {id_pedido} -> NENHUMA RESERVA ENCONTRADA PARA DEVOLVER")

# Processa o evento "pagamento.recusado" e devolve os produtos reservados ao estoque
def processar_pagamento_recusado(ch, id_pedido):
    devolver_reserva(id_pedido)
    enviar_evento_estoque(ch, 'estoque.devolvido', {
        "id_pedido": id_pedido,
        "status": "ESTOQUE_DEVOLVIDO"
    })

# Callback para processar mensagens recebidas do RabbitMQ
def callback(ch, method, properties, body):
    envelope = json.loads(body.decode('utf-8'))

    if not verificar_evento(envelope, CAMINHO_CHAVES_PUBLICAS):
        print(f"    Assinatura inválida! Evento '{method.routing_key}' descartado.")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    dados = envelope.get('payload', {})
    id_pedido = dados.get('id_pedido')
    itens = dados.get('produtos') or dados.get('itens') or []
    print("-" * 40)
    print(f"Routing Key: {method.routing_key}")
    print(f"ID do Pedido: {id_pedido}")

    match method.routing_key:
        case 'pedido.criado':
            processar_pedido_criado(ch, id_pedido, itens)
        case 'pedido.excluido':
            processar_pedido_excluido(ch, id_pedido)
        case 'pagamento.recusado':
            processar_pagamento_recusado(ch, id_pedido)

    print("-" * 40)
    ch.basic_ack(delivery_tag=method.delivery_tag)

if __name__ == '__main__':
    iniciar_ms_estoque()
