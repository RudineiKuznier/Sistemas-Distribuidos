import pika
import sys

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', virtual_host='my_vhost'))
channel = connection.channel()

channel.exchange_declare(exchange='promocoes', exchange_type='topic')

result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

binding_keys = sys.argv[1:]
if not binding_keys:
    sys.stderr.write("Uso: %s [binding_key]...\n" % sys.argv[0])
    sys.exit(1)

for binding_key in binding_keys:
    channel.queue_bind(
        exchange='promocoes', queue=queue_name, routing_key=binding_key)

print('     Aguardando promoções. Para sair pressione CTRL+C')

def callback(ch, method, properties, body):
    print(f"    Recebido {method.routing_key}:{body.decode()}")

channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()