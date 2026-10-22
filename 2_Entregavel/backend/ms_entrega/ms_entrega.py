import pika
import json
import time 
import random
import os
from security import verificar_evento, assinar_evento

NOME_PRODUTOR = "ms_entrega"

CAMINHO_CHAVE_PRIVADA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    f"{NOME_PRODUTOR}.pem"
)

CAMINHO_CHAVES_PUBLICAS = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "chaves_publicas"
)

def enviar_evento_entrega(channel, routing_key, dados_evento):
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

def processar_entrega(channel, id_pedido):

    numero_nf = f"NF-{random.randint(10000, 99999)}"
    codigo_rastreio = f"BR{random.randint(100000000, 999999999)}BR"

    message = {
        "id_pedido": id_pedido,
        "status": "PEDIDO_ENVIADO",
        "numero_nota_fiscal": numero_nf,
        "codigo_rastreio": codigo_rastreio
    }
    time.sleep(3.0)

    enviar_evento_entrega(channel, 'pedido.enviado', message)
     
    
def iniciar_ms_entrega():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', virtual_host='my_vhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='eCommerce', exchange_type='direct')

    queue_name = 'ms_fila_entrega'

    channel.queue_declare(queue=queue_name, durable=True)

    channel.queue_bind(exchange='eCommerce', queue=queue_name, routing_key='pagamento.aprovado')

    print(' [*] Aguardando eventos de entrega. Para sair pressione CTRL+C')

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=queue_name, on_message_callback=callback, auto_ack=False)

    channel.start_consuming()

        
def callback(ch, method, properties, body):
    if (method.routing_key == 'pagamento.aprovado'):
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
                
        processar_entrega(ch, id_pedido)
    
        print("-" * 40)
    
    ch.basic_ack(delivery_tag=method.delivery_tag)
        
if __name__ == '__main__':
    iniciar_ms_entrega()