import pika
import json
import sys

def pub_pedido_excluido(channel, id_pedido, motivo):
    payload = {
        "id_pedido": id_pedido,
        "motivo": motivo,
        "status": "EXCLUIDO"
    }
    message = json.dumps(payload)

    channel.basic_publish(
        exchange='eCommerce',
        routing_key='pedido.excluido',
        body=message,
        properties=pika.BasicProperties(
            content_type='application/json',
            delivery_mode=pika.DeliveryMode.Persistent
        )
    )
    print(f"    Evento: pedido.excluido (ID: {id_pedido} | Motivo: {motivo})")

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', virtual_host='my_vhost'))
channel = connection.channel()

channel.exchange_declare(exchange='eCommerce', exchange_type='direct')

queue_name = 'ms_fila_principal'

channel.queue_declare(queue=queue_name, durable=True)

eventos = [
    'pagamento.aprovado',
    'pagamento.recusado',
    'pedido.enviado',
    'pedido.estoque_ok',
    'estoque.indisponivel'
]

for routing_key in eventos:
    channel.queue_bind(
        exchange='eCommerce', queue=queue_name, routing_key=routing_key)

print(' [*] Aguardando eventos de eCommerce. Para sair pressione CTRL+C')

def callback(ch, method, properties, body):
    dados = json.loads(body.decode('utf-8'))
    id_pedido = dados.get('id_pedido')
    print("-" * 40);
    print(f"Routing Key: {method.routing_key}")
    print(f"ID do Pedido: {id_pedido}")


    match method.routing_key:
        case 'pagamento.aprovado':
            print(f"    Status: Pedido {id_pedido} -> PAGAMENTO_APROVADO")

        case 'pedido.enviado':
            print(f"    Status: Pedido {id_pedido} -> PEDIDO ENVIADO")

        case 'pedido.estoque_ok':
            print(f"    Status: Pedido {id_pedido} -> PEDIDO ESTOQUE OK")

        case 'pagamento.recusado':
            print(f"    Status: Pedido {id_pedido} -> PAGAMENTO_RECUSADO")
            pub_pedido_excluido(ch, id_pedido, motivo="Pagamento Recusado")

        case 'estoque.indisponivel':
            print(f"    Status Atualizado: Pedido {dados.get('id_pedido')} -> ESTOQUE_INDISPONIVEL")
            pub_pedido_excluido(ch, id_pedido, motivo="Estoque Indisponível") 

    print("-" * 40)


channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()