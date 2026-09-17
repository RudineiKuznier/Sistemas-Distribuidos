import json
import random
import sys
import time
import pika

CATEGORIAS = ["A", "B", "C"]
EXCHANGE = "promocoes"
INTERVALO_PADRAO = 2

# Cria uma promoção aleatória para a categoria especificada
def criar_promocao(categoria):
    produto = random.choice(["camisa", "tenis", "calca", "meia", "bone"])
    return {
        "produto": produto,
        "categoria": categoria,
        "desconto_percentual": random.randint(10, 50),
    }

# Publica promoções no exchange "promocoes" com a routing key especificada ou aleatória
def publicar_promocoes(routing_key=None, intervalo=INTERVALO_PADRAO):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", virtual_host="my_vhost")
    )
    channel = connection.channel()
    channel.exchange_declare(
        exchange=EXCHANGE,
        exchange_type="topic")

    print("[*] Publicando promoções. Para sair pressione CTRL+C")

    try:
        while True:
            chave = routing_key or f"promocao.categoria.{random.choice(CATEGORIAS)}"
            categoria = chave.rsplit(".", 1)[-1]
            promocao = criar_promocao(categoria)

            channel.basic_publish(
                exchange=EXCHANGE,
                routing_key=chave,
                body=json.dumps(promocao),
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=pika.DeliveryMode.Persistent,
                ),
            )
            print(f"[x] Enviado {chave}: {promocao}")
            time.sleep(intervalo)
    finally:
        connection.close()

if __name__ == "__main__":
    chave = sys.argv[1] if len(sys.argv) > 1 else None
    intervalo = float(sys.argv[2]) if len(sys.argv) > 2 else INTERVALO_PADRAO
    publicar_promocoes(chave, intervalo)