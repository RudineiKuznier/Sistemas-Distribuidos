import pika
import json
import sys

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', virtual_host='my_vhost'))
channel = connection.channel()

channel.exchange_declare(exchange='eCommerce', exchange_type='direct')

result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

severities = sys.argv[1:]
if not severities:
    sys.stderr.write("Uso: %s [severities]...\n" % sys.argv[0])
    sys.exit(1)

for severity in severities:
    channel.queue_bind(
        exchange='eCommerce', queue=queue_name, routing_key=severity)

print(' [*] Aguardando eventos de eCommerce. Para sair pressione CTRL+C')

def callback(ch, method, properties, body):
    dados = json.loads(body.decode('utf-8'))
    print("-" * 40);
    print(f"Routing Key: {method.routing_key}")
    print(f"ID do Pedido: {dados.get('id_pedido')}")
    print(f"Data: {dados.get('criacao')}")
    print(f"Produtos: {dados.get('produtos')}")
    print("-" * 40)


channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()