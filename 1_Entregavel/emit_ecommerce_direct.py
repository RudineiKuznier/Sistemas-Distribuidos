import pika
import sys

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', virtual_host='my_vhost'))
channel = connection.channel()

channel.exchange_declare(exchange='eCommerce', exchange_type='direct')

severity = sys.argv[1] if len(sys.argv) > 1 else 'info'
message = ' '.join(sys.argv[2:]) or 'Novo evento'

channel.basic_publish(
    exchange='eCommerce',
    routing_key=severity,
    body=message
)

print(f" [x] Enviado para eCommerce {severity}: {message}")
connection.close()