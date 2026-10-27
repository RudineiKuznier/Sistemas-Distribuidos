import os
import json
import random
import threading
import time
import pika
import resend
from dotenv import load_dotenv
from security import verificar_evento

load_dotenv()
resend.api_key = os.getenv("RESEND_API_KEY")

BASE = os.path.dirname(os.path.abspath(__file__))
CAMINHO_CHAVES_PUBLICAS = os.path.join(BASE, "chaves_publicas")

INTERESSES = {}         

def enviar_email(email, categoria, desconto):
    try:
        resend.Emails.send({
            "from": "onboarding@resend.dev",
            "to": email,
            "subject": f"Promoção: {desconto}% em {categoria}!",
            "html": f"<p>Aproveite <strong>{desconto}% de desconto</strong> em {categoria}.</p>"
        })
        print(f"    E-mail enviado para {email} ({categoria}, {desconto}%)")
    except Exception as e:
        print(f"    [Resend Error] {email}: {e}")


def gerador_promocoes():
    while True:
        time.sleep(60)

        interesses = dict(INTERESSES) 
        categorias = {c for cats in interesses.values() for c in cats}
        if not categorias:
            continue

        categoria = random.choice(sorted(categorias))
        destinatarios = [e for e, cats in interesses.items() if categoria in cats]

        print(f"[Promoção] 10% em {categoria} -> {len(destinatarios)} cliente(s)")
        for email in destinatarios:
            enviar_email(email, categoria, 10)

def callback(ch, method, properties, body):
    try:
        envelope = json.loads(body.decode('utf-8'))

        if not verificar_evento(envelope, CAMINHO_CHAVES_PUBLICAS):
            print(f"    Assinatura inválida! Evento '{method.routing_key}' descartado.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        dados = envelope.get('payload', {})
        email = dados.get('email')
        print("-" * 40)
        print(f"Routing Key: {method.routing_key}")
        print(f"E-mail: {email}")

        if dados.get('acao') == 'cancelar':
            INTERESSES.pop(email, None)
        else:
            INTERESSES[email] = dados.get('categorias', [])
                
        print("-" * 40)
    except Exception as e:
        print(f"    [Erro callback promoções] {e}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def iniciar_ms_promocoes():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', virtual_host='my_vhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='eCommerce', exchange_type='direct')

    queue_name = 'ms_fila_promocoes'
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(exchange='eCommerce', queue=queue_name, routing_key='interesse.promocao')

    print('     Aguardando eventos de promoções. Para sair pressione CTRL+C')
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
    channel.start_consuming()


if __name__ == '__main__':
    threading.Thread(target=gerador_promocoes, daemon=True).start()
    iniciar_ms_promocoes()