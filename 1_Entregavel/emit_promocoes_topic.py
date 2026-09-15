import pika
import sys

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost',  virtual_host='my_vhost'))
channel = connection.channel()

channel.exchange_declare(exchange='promocoes', exchange_type='topic')

severity = sys.argv[1] if len(sys.argv) > 1 else 'promocao.generica'
message = ' '.join(sys.argv[2:]) or 'Nova promocao'

channel.basic_publish(
    exchange='promocoes',
    routing_key=severity, 
    body=message
)
print(f" [x] Enviado para promocoes {message}")
connection.close()