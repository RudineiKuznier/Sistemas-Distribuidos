import os
import json
import pika
from datetime import datetime

PEDIDOS = {}

def criar_conexao():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", virtual_host="my_vhost")
    )
    return connection

# Envia o evento para o exchange "eCommerce" com a routing key especificada
def enviar_evento_ecommerce(routing_key, dados_evento):
    connection = criar_conexao()

    message = json.dumps(dados_evento)

    try:
        channel = connection.channel()
        channel.exchange_declare(exchange="eCommerce", exchange_type="direct")
        channel.basic_publish(
            exchange="eCommerce",
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=pika.DeliveryMode.Persistent,
            ),
        )
        print(f"    Enviado para eCommerce {routing_key}: {dados_evento}")
    finally:
        connection.close()

def visualizar_produtos():
    print("\nO catálogo é mantido pelo microsserviço Estoque.")
    print("Consulte o estoque configurado em ms_estoque.py.")
    input("\nPressione ENTER para voltar ao menu...")

# Solicita ao usuário os dados do pedido e envia o evento "pedido.criado" para o exchange "eCommerce"
def realizar_pedido():
    limpar_tela()
    id_pedido = input("ID do Pedido: ").strip()
    produto = input("Nome do Produto: ").strip()
    qtd = int(input("Quantidade: ").strip())

    data_hora_criacao = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    payload = {
        "id_pedido": id_pedido,
        "produtos": [{"nome": produto, "quantidade": qtd}],
        "criacao": data_hora_criacao,
        "status": "CRIADO",
    }

    PEDIDOS[id_pedido] = payload
    enviar_evento_ecommerce("pedido.criado", payload)

def obter_pedido(id_pedido):
    return PEDIDOS.get(id_pedido)

def listar_pedidos():
    return list(PEDIDOS.values())

# Envia o evento "pedido.excluido" para o exchange "eCommerce" usando ID do pedido
def excluir_pedido():
    id_pedido = input("ID do Pedido: ").strip()
    pedido = obter_pedido(id_pedido)
    if pedido is None:
        input("\nPedido não encontrado. Pressione ENTER para voltar...")
        return

    pedido["status"] = "EXCLUIDO"

    enviar_evento_ecommerce(
        "pedido.excluido",
        {
            "id_pedido": id_pedido,
            "produtos": pedido.get("produtos", []),
            "motivo": "Exclusão solicitada pelo usuário",
            "status": "EXCLUIDO",
        },
    )
    input("\nPedido excluído. Pressione ENTER para voltar...")

# Lista os pedidos registrados
def consultar_pedidos():
    pedidos = listar_pedidos()
    print("\nPedidos:")
    if not pedidos:
        print("Nenhum pedido registrado.")
    else:
        for pedido in pedidos:
            print(
                f"- {pedido['id_pedido']}: {pedido.get('status', 'SEM_STATUS')} "
                f"{pedido.get('produtos', [])}"
            )
    input("\nPressione ENTER para voltar ao menu...")

# Publica pedido.excluido no exchange eCommerce
def pub_pedido_excluido(channel, id_pedido, produtos, motivo):
    payload = {
        "id_pedido": id_pedido,
        "produtos": produtos,
        "motivo": motivo,
        "status": "EXCLUIDO",
    }
    channel.basic_publish(
        exchange="eCommerce",
        routing_key="pedido.excluido",
        body=json.dumps(payload),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=pika.DeliveryMode.Persistent,
        ),
    )
    print(f"    Evento: pedido.excluido (ID: {id_pedido} | Motivo: {motivo})")

# Processa eventos recebidos, atualiza o status do pedido e confirma a mensagem.
def callback(ch, method, body):
    dados = json.loads(body.decode("utf-8"))
    id_pedido = dados.get("id_pedido")
    print("-" * 40);
    print(f"Routing Key: {method.routing_key}")
    print(f"ID do Pedido: {id_pedido}")

    match method.routing_key:
        case 'pagamento.aprovado':
            status = "PAGAMENTO_APROVADO"
            motivo = None
        case 'pagamento.recusado':
            status = "PAGAMENTO_RECUSADO"
            motivo = "Pagamento recusado"
        case 'pedido.enviado':
            status = "PEDIDO_ENVIADO"
            motivo = None
        case 'pedido.estoque_ok':
            status = "ESTOQUE_OK"
            motivo = None
        case 'estoque.indisponivel':
            status = "ESTOQUE_INDISPONIVEL"
            motivo = "Estoque indisponível"
        case _:
            print(f"Routing key não suportada: {method.routing_key}")

    pedido = obter_pedido(id_pedido)

    if pedido is None:
        pedido = {
            "id_pedido": id_pedido,
            "produtos": dados.get("produtos", []),
            "status": status,
        }
    else:
        pedido["status"] = status

    PEDIDOS[id_pedido] = pedido

    print(f"Status: Pedido {id_pedido} -> {status}")
    if motivo is not None:
        pub_pedido_excluido(
            ch, id_pedido, pedido.get("produtos", []), motivo
        )
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Configura o consumidor para receber eventos do exchange "eCommerce"
def configurar_consumidor():
    connection = criar_conexao()
    channel = connection.channel()
    channel.exchange_declare(exchange="eCommerce", exchange_type="direct")
    channel.queue_declare(queue="ms_fila_principal", durable=True)

    for routing_key in (
        "pagamento.aprovado",
        "pagamento.recusado",
        "pedido.enviado",
        "pedido.estoque_ok",
        "estoque.indisponivel",
    ):
        channel.queue_bind(exchange="eCommerce", queue="ms_fila_principal", routing_key=routing_key)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue="ms_fila_principal",
        on_message_callback=callback,
        auto_ack=False,
    )
    return connection, channel

def limpar_tela():
    os.system("cls" if os.name == "nt" else "clear")

def menu(connection):
    while True:

        #processa eventos pendentes antes de exibir o menu
        connection.process_data_events(time_limit=0)

        limpar_tela()
        print("*" * 40)
        print("1. Visualizar Produtos")
        print("2. Realizar Pedido")
        print("3. Excluir Pedido")
        print("4. Consultar Pedidos")
        print("0. Sair")
        print("*" * 40)

        opcao = input("Escolha uma opção: ").strip()

        #processa novamente eventos pendentes antes de executar a ação escolhida
        connection.process_data_events(time_limit=0)

        match opcao:
            case "1":
                visualizar_produtos()
            case "2":
                realizar_pedido()
            case "3":
                excluir_pedido()
            case "4":
                consultar_pedidos()
            case "0":
                print("Encerrando Microsserviço Principal...")
                break
            case _:
                input("\nOpção inválida. Pressione ENTER para voltar...")

if __name__ == "__main__":
    connection, channel = configurar_consumidor()
    menu(connection)
    connection.close()