import pika
import json
import random
from security import verificar_evento, assinar_evento
import os

NOME_PRODUTOR = "ms_pagamento"

CAMINHO_CHAVE_PRIVADA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    f"{NOME_PRODUTOR}.pem"
)

CAMINHO_CHAVES_PUBLICAS = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "chaves_publicas"
)


def enviar_evento_pagamento(channel, routing_key, dados_evento):
    envelope_assinado = assinar_evento(
        dados_evento,
        NOME_PRODUTOR,
        CAMINHO_CHAVE_PRIVADA
    )

    message = json.dumps(envelope_assinado)
    channel.basic_publish(
        exchange='eCommerce',
        routing_key=routing_key,
        body=message,
        properties=pika.BasicProperties(
            content_type='application/json',
            delivery_mode=pika.DeliveryMode.Persistent
        )
    )
    print(f"    Enviado para eCommerce {routing_key}: {message}")

def processar_pagamento(channel, id_pedido):
    aprovado = random.choice([True, False])

    if (aprovado):
        message = {
            "id_pedido": id_pedido,
            "status": "PAGAMENTO_APROVADO"
        }
        enviar_evento_pagamento(channel, 'pagamento.aprovado', message)
    else:
        message = {
            "id_pedido": id_pedido, 
            "status": "PAGAMENTO_RECUSADO", 
            "motivo": "Saldo insuficiente"
        }
        enviar_evento_pagamento(channel, 'pagamento.recusado', message)    
    
def iniciar_ms_pagamento():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', virtual_host='my_vhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='eCommerce', exchange_type='direct')

    queue_name = 'ms_fila_pagamento'

    channel.queue_declare(queue=queue_name, durable=True)

    channel.queue_bind(exchange='eCommerce', queue=queue_name, routing_key='pedido.estoque_ok')

    print('     Aguardando eventos de pagamento. Para sair pressione CTRL+C')

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=queue_name, on_message_callback=callback, auto_ack=False)

    channel.start_consuming()

        
def callback(ch, method, properties, body):
    if (method.routing_key == 'pedido.estoque_ok'):
        envelope = json.loads(body.decode('utf-8'))

        if not verificar_evento(envelope, CAMINHO_CHAVES_PUBLICAS):
            print(f"    Assinatura inválida! Evento '{method.routing_key}' descartado.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        dados = envelope.get('payload', {})
        id_pedido = dados.get('id_pedido')
        print("-" * 40);
        print(f"Routing Key: {method.routing_key}")
        print(f"ID do Pedido: {id_pedido}")
                
        processar_pagamento(ch, id_pedido)
    
        print("-" * 40)
    
    ch.basic_ack(delivery_tag=method.delivery_tag)
        
if __name__ == '__main__':
    iniciar_ms_pagamento()