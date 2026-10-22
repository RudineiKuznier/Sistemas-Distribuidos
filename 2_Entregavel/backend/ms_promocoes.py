import html
import json
import os
import random
import sqlite3
import threading
from contextlib import closing

import pika
import resend
import requests
from dotenv import load_dotenv
from security import verificar_evento, assinar_evento

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

NOME_PRODUTOR = "ms_promocoes"
CAMINHO_BANCO = os.path.join(os.path.dirname(os.path.abspath(__file__)), "promocoes.db")
CAMINHO_CHAVE_PRIVADA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    f"{NOME_PRODUTOR}.pem"
)
CAMINHO_CHAVES_PUBLICAS = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "chaves_publicas"
)
URL_ESTOQUE = os.getenv("STOCK_API_URL", "http://localhost:5001/api/products")

def conectar_banco():
    return sqlite3.connect(CAMINHO_BANCO, timeout=5)

def inicializar_banco():
    with closing(conectar_banco()) as connection:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                email TEXT PRIMARY KEY,
                categories TEXT NOT NULL
            )
        """)
        connection.commit()

def salvar_inscricao(email, categories):
    with closing(conectar_banco()) as connection:
        connection.execute(
            """INSERT INTO subscriptions (email, categories)
               VALUES (?, ?)
               ON CONFLICT(email) DO UPDATE SET categories = excluded.categories""",
            (email, json.dumps(categories, ensure_ascii=False))
        )
        connection.commit()

def cancelar_inscricao(email):
    with closing(conectar_banco()) as connection:
        cursor = connection.execute(
            "DELETE FROM subscriptions WHERE email = ?",
            (email,)
        )
        connection.commit()
        return cursor.rowcount > 0

def listar_inscritos(category):
    with closing(conectar_banco()) as connection:
        subscriptions = connection.execute(
            "SELECT email, categories FROM subscriptions ORDER BY email"
        ).fetchall()

    return [
        email
        for email, categories_json in subscriptions
        if not (categories := json.loads(categories_json)) or category in categories
    ]

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

def buscar_produto(category):
    response = requests.get(URL_ESTOQUE, timeout=3)
    response.raise_for_status()
    products = response.json()
    matching_products = [
        product
        for product in products
        if product.get("category") == category and product.get("stock", 0) > 0
    ]
    if not matching_products:
        raise ValueError(f"Não há produtos disponíveis na categoria {category}.")
    return random.choice(matching_products)

def enviar_campanha(campaign):
    api_key = os.getenv("RESEND_API_KEY")
    sender = os.getenv("RESEND_FROM_EMAIL")
    if not api_key or not sender:
        raise RuntimeError("Configure RESEND_API_KEY e RESEND_FROM_EMAIL no .env.")

    category = campaign["category"]
    product = buscar_produto(category)
    recipients = listar_inscritos(category)
    if not recipients:
        print(f"Campanha {campaign['campaign_id']}: nenhum inscrito para {category}.")
        return

    resend.api_key = api_key
    discount_percent = campaign["discount_percent"]
    product_name = html.escape(str(product["name"]))
    price = float(product["price"])
    discounted_price = price * (100 - discount_percent) / 100
    subject = f"Oferta de {category}: {discount_percent}% de desconto"
    message_html = (
        f"<p>Promoção em <strong>{product_name}</strong>: "
        f"{discount_percent}% de desconto.</p>"
        f"<p>De R$ {price:.2f} por R$ {discounted_price:.2f}.</p>"
    )

    falhas = []
    for email in recipients:
        try:
            resend.Emails.send({
                "from": sender,
                "to": [email],
                "subject": subject,
                "html": message_html,
            })
            print(f"Campanha {campaign['campaign_id']} enviada para {email}.")
        except Exception as erro:
            falhas.append(email)
            print(f"[Resend Error] Falha ao enviar para {email}: {erro}")

    if falhas:
        raise RuntimeError(
            f"Falha ao enviar campanha para {len(falhas)} destinatário(s)."
        )

def callback(ch, method, properties, body):
    try:
        envelope = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as erro:
        print(f"[Promotions Consumer Error] Mensagem inválida: {erro}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    if (
        not isinstance(envelope, dict)
        or envelope.get("produtor") != "ms_principal"
        or not verificar_evento(envelope, CAMINHO_CHAVES_PUBLICAS)
    ):
        print(f"Assinatura inválida! Evento '{method.routing_key}' descartado.")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        print("Evento de promoções sem payload válido.")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    try:
        if method.routing_key == "interesse.promocao":
            email = payload.get("email")
            categories = payload.get("categories")
            if (
                not isinstance(email, str)
                or not email.strip()
                or not isinstance(categories, list)
                or any(not isinstance(category, str) or not category.strip() for category in categories)
            ):
                print("Dados de inscrição inválidos.")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return
            salvar_inscricao(email, categories)
            print(f"Inscrição de {email} registrada.")
        elif method.routing_key == "interesse.promocao.cancelado":
            email = payload.get("email")
            if not isinstance(email, str) or not email.strip():
                print("E-mail de cancelamento inválido.")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return
            if cancelar_inscricao(email):
                print(f"Inscrição de {email} cancelada.")
            else:
                print(f"Nenhuma inscrição encontrada para {email}.")
        elif method.routing_key == "promocao.solicitada":
            campaign_id = payload.get("campaign_id")
            category = payload.get("category")
            discount_percent = payload.get("discount_percent")
            if (
                not isinstance(campaign_id, str)
                or not isinstance(category, str)
                or not isinstance(discount_percent, int)
                or isinstance(discount_percent, bool)
                or not 1 <= discount_percent <= 90
            ):
                print("Dados da campanha inválidos.")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return
            enviar_campanha(payload)
        else:
            print(f"Evento não suportado: {method.routing_key}.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return
    except ValueError as erro:
        print(f"[Promotions Consumer Error] {erro}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return
    except requests.RequestException as erro:
        print(f"[Promotions Consumer Error] {erro}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return
    except Exception as erro:
        print(f"[Promotions Consumer Error] {erro}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    ch.basic_ack(delivery_tag=method.delivery_tag)

def consumir_eventos_promocoes():
    while True:
        connection = None
        try:
            connection = criar_conexao_rabbitmq()
            channel = connection.channel()
            channel.exchange_declare(exchange="eCommerce", exchange_type="direct")
            channel.queue_declare(queue="ms_fila_promocoes", durable=True)
            for routing_key in (
                "interesse.promocao",
                "interesse.promocao.cancelado",
                "promocao.solicitada",
            ):
                channel.queue_bind(
                    exchange="eCommerce",
                    queue="ms_fila_promocoes",
                    routing_key=routing_key
                )

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue="ms_fila_promocoes",
                on_message_callback=callback,
                auto_ack=False
            )
            print("Aguardando inscrições e campanhas de promoções.")
            channel.start_consuming()
        except Exception as erro:
            print(f"[RabbitMQ Consumer Offline] Reconectando em 5s... Erro: {erro}")
            threading.Event().wait(5)
        finally:
            if connection and connection.is_open:
                connection.close()

def iniciar_ms_promocoes():
    if not os.path.isfile(CAMINHO_CHAVE_PRIVADA):
        raise FileNotFoundError(
            f"Chave privada do MS Promoções não encontrada: {CAMINHO_CHAVE_PRIVADA}"
        )
    inicializar_banco()
    consumir_eventos_promocoes()

if __name__ == "__main__":
    iniciar_ms_promocoes()
