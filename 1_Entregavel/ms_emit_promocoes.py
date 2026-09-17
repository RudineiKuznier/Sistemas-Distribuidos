import pika
import sys
import random

CATEGORIA = ['A','B','C']

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost',  virtual_host='my_vhost'))
channel = connection.channel()

channel.exchange_declare(exchange='promocoes', exchange_type='topic')

routing_key = f'promocao.categoria.{random.choice(CATEGORIA)}'

message = ' '.join(sys.argv[1:]) or 'Mensagem de promoção!'

channel.basic_publish(
    exchange='promocoes',
    routing_key=routing_key, 
    body=message
)
print(f"    Enviado {routing_key} : {message}")
connection.close()