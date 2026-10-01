import pika
import sys

def iniciar_ms_promocoes():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', virtual_host='my_vhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='promocoes', exchange_type='topic')

    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    binding_keys = sys.argv[1:]
    if not binding_keys:
        sys.stderr.write("Uso: %s [binding_key]...\nExemplo: %s promocao.categoria.A\n" % (sys.argv[0], sys.argv[0]))
        sys.exit(1)

    for binding_key in binding_keys:
        channel.queue_bind(
            exchange='promocoes', queue=queue_name, routing_key=binding_key)

    print('     Aguardando promoções. Para sair pressione CTRL+C')


    def callback(ch, method, properties, body):
        print(f"    Recebido {method.routing_key}:{body.decode()}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(
        queue=queue_name, on_message_callback=callback, auto_ack=False)

    channel.start_consuming()

if __name__ == "__main__":
    iniciar_ms_promocoes()
