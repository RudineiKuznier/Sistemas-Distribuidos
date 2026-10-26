import pika
import json
from security import verificar_evento, assinar_evento
import os
import csv

CAMINHO_CSV = os.path.abspath(os.getenv(
    "DATABASE_CSV",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "database.csv")
))

def carregar_estoque():
    with open(CAMINHO_CSV, encoding="utf-8-sig", newline="") as f:
        leitor = csv.DictReader(f)
        campos = leitor.fieldnames
        linhas = list(leitor)
    return campos, linhas

CAMPOS, LINHAS = carregar_estoque()
ESTOQUE = {l["id"]: int(l["stock"]) for l in LINHAS}

def salvar_estoque():
    for l in LINHAS:
        l["stock"] = str(ESTOQUE[l["id"]])
    tmp = CAMINHO_CSV + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        escritor = csv.DictWriter(f, fieldnames=CAMPOS)
        escritor.writeheader()
        escritor.writerows(LINHAS)
    os.replace(tmp, CAMINHO_CSV)

RESERVAS = {}

NOME_PRODUTOR = "ms_estoque"

CAMINHO_CHAVE_PRIVADA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    f"{NOME_PRODUTOR}.pem"
)

CAMINHO_CHAVES_PUBLICAS = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "chaves_publicas"
)

def iniciar_ms_estoque():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', virtual_host='my_vhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='eCommerce', exchange_type='direct')

    queue_name = 'ms_fila_estoque'

    channel.queue_declare(queue=queue_name, durable=True)

    channel.queue_bind(exchange='eCommerce', queue=queue_name, routing_key='pedido.criado')
    channel.queue_bind(exchange='eCommerce', queue=queue_name, routing_key='pedido.excluido')
    channel.queue_bind(exchange='eCommerce', queue=queue_name, routing_key='pagamento.recusado')

    print(' [*] Aguardando eventos de estoque. Para sair pressione CTRL+C')

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=queue_name, on_message_callback=callback, auto_ack=False)

    channel.start_consuming()

# Envia eventos para o exchange "eCommerce" com a routing key especificada
def enviar_evento_estoque(channel, routing_key, dados_evento):
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

# Processa o evento "pedido.criado" verificando a disponibilidade de estoque e reservando os produtos
def processar_pedido_criado(ch, id_pedido, itens):
    for item in itens:
        prod = str(item.get("nome"))
        try:
            qtd = int(item.get("quantidade"))
        except (TypeError, ValueError):
            qtd = 0

        if qtd <= 0 or ESTOQUE.get(prod, 0) < qtd:
            print(f"    Status: Pedido {id_pedido} -> ESTOQUE INDISPONÍVEL ({prod})")
            enviar_evento_estoque(ch, 'estoque.indisponivel', {
                "id_pedido": id_pedido,
                "status": "ESTOQUE_INDISPONIVEL",
                "motivo": f"Produto {prod} sem estoque suficiente"
            })
            return

    for item in itens:
        ESTOQUE[str(item["nome"])] -= int(item["quantidade"])

    RESERVAS[id_pedido] = list(itens)
    salvar_estoque()

    print(f"    Status: Pedido {id_pedido} -> ESTOQUE RESERVADO COM SUCESSO")
    for item in itens:
        p = str(item["nome"])
        print(f"    [Estoque Atual de '{p}']: {ESTOQUE[p]}")

    enviar_evento_estoque(ch, 'pedido.estoque_ok', {
        "id_pedido": id_pedido,
        "produtos": itens,
        "status": "ESTOQUE_OK"
    })


def processar_pedido_excluido(id_pedido):
    itens = RESERVAS.pop(id_pedido, [])

    if itens:
        for item in itens:
            prod = str(item.get("nome"))
            qtd = int(item.get("quantidade", 1))
            if prod in ESTOQUE:
                ESTOQUE[prod] += qtd
        salvar_estoque()
        print(f"    Status: Pedido {id_pedido} -> PRODUTOS DEVOLVIDOS AO ESTOQUE")
    else:
        print(f"    Status: Pedido {id_pedido} -> NENHUMA RESERVA ENCONTRADA PARA DEVOLVER")


def callback(ch, method, properties, body):
    envelope = json.loads(body.decode('utf-8'))

    if not verificar_evento(envelope, CAMINHO_CHAVES_PUBLICAS):
        print(f"    Assinatura inválida! Evento '{method.routing_key}' descartado.")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    dados = envelope.get('payload', {})
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
            case 'pagamento.recusado':
                processar_pedido_excluido(id_pedido)

    print("-" * 40)
    ch.basic_ack(delivery_tag=method.delivery_tag)

if __name__ == '__main__':
    iniciar_ms_estoque()