import pika
import json

#banco simulado
ESTOQUE = {
    "camisa": 100,
    "tenis": 2,
    "calca": 0,
    "meia": 10,
    "bone": 1,
    "casaco": 9,
    "bermuda": 110,
    "cinto": 50,
    "sapato": 40,
    "mochila": 1
}

RESERVAS = {}

def iniciar_ms_estoque():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', virtual_host='my_vhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='eCommerce', exchange_type='direct')

    queue_name = 'ms_fila_estoque'

    channel.queue_declare(queue=queue_name, durable=True)

    channel.queue_bind(exchange='eCommerce', queue=queue_name, routing_key='pedido.criado')
    channel.queue_bind(exchange='eCommerce', queue=queue_name, routing_key='pedido.excluido')

    print(' [*] Aguardando eventos de estoque. Para sair pressione CTRL+C')

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=queue_name, on_message_callback=callback, auto_ack=False)

    channel.start_consuming()

# Envia eventos para o exchange "eCommerce" com a routing key especificada
def enviar_evento_estoque(channel, routing_key, dados_evento):
    message = json.dumps(dados_evento)
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

# Processa o evento "pedido.criado" verificando a disponibilidade de estoque e reservando os produtos
def processar_pedido_criado(ch, id_pedido, itens):
    for item in itens:
        prod = item["nome"]
        qtd = item["quantidade"]
        
        if not prod or ESTOQUE.get(prod, 0) < qtd:
            print(f"    Status: Pedido {id_pedido} -> ESTOQUE INDISPONÍVEL ({prod})")
            enviar_evento_estoque(ch, 'estoque.indisponivel', {
                "id_pedido": id_pedido,
                "status": "ESTOQUE_INDISPONIVEL",
                "motivo": f"Produto {prod} sem estoque suficiente"
            })
            return
        
    for item in itens:
        prod = item["nome"]
        qtd = item["quantidade"]
        ESTOQUE[prod] -= qtd

    RESERVAS[id_pedido] = list(itens)

    print(f"    Status: Pedido {id_pedido} -> ESTOQUE RESERVADO COM SUCESSO")
    print(f"    [Estoque Atual de '{prod}']: {ESTOQUE[prod]}")
    enviar_evento_estoque(ch, 'pedido.estoque_ok', {
        "id_pedido": id_pedido,
        "produtos": itens,
        "status": "ESTOQUE_OK"
    })

# Processa o evento "pedido.excluido" e devolve os produtos reservados ao estoque
def processar_pedido_excluido(id_pedido):
    itens = RESERVAS.pop(id_pedido, [])
    
    if itens:
        for item in itens:
            prod = item.get("produto") or item.get("nome")
            qtd = item.get("quantidade", 1)
            if prod in ESTOQUE:
                ESTOQUE[prod] += qtd
        print(f"    Status: Pedido {id_pedido} -> PRODUTOS DEVOLVIDOS AO ESTOQUE")
    else:
        print(f"    Status: Pedido {id_pedido} -> NENHUMA RESERVA ENCONTRADA PARA DEVOLVER")

def callback(ch, method, properties, body):
    dados = json.loads(body.decode('utf-8'))
    id_pedido = dados.get('id_pedido')
    itens = dados.get('produtos') or dados.get('itens') or []
    print("-" * 40);
    print(f"Routing Key: {method.routing_key}")
    print(f"ID do Pedido: {id_pedido}")

    match method.routing_key:
            case 'pedido.criado':
                processar_pedido_criado(ch, id_pedido, itens)
            case 'pedido.excluido':
                processar_pedido_excluido(id_pedido)

    print("-" * 40)
    ch.basic_ack(delivery_tag=method.delivery_tag)

if __name__ == '__main__':
    iniciar_ms_estoque()