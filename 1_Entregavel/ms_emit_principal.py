import pika
import sys
import os
import json
from datetime import datetime, timezone

def enviar_evento_ecommerce(routing_key, dados_evento):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', virtual_host='my_vhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='eCommerce', exchange_type='direct')

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

    input(f"    Enviado para eCommerce {routing_key}: {message}")
    connection.close()
    menu()

def limpar_tela():
    os.system('cls' if os.name == 'nt' else 'clear')

def realizar_pedido():
    limpar_tela()
    id_pedido = input("ID do Pedido: ").strip()
    produto = input("Nome do Produto: ").strip()
    qtd = int(input("Quantidade: ").strip())

    data_hora_criacao = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    payload = {
        "id_pedido": id_pedido,
        "produtos": [{"nome": produto, "quantidade": qtd}],
        "criacao": data_hora_criacao
    }

    enviar_evento_ecommerce('pedido.criado', payload)

def menu():
    while True:
        limpar_tela()
        print("*" * 40);
        print("1. Visualizar Produtos")
        print("2. Realizar Pedido")
        print("3. Excluir Pedido")
        print("4. Consultar Pedidos")
        print("0. Sair")
        print("*" * 40);

        opcao = input("Escolha uma opção: ").strip()
        match opcao:
            case '1':
                input("\nLista de produtos. Pressione ENTER para tentar novamente...")
            case '2':
                realizar_pedido()
            case '0':
                print("Encerrando Microsserviço Principal...")
                break
            case _:
               input("\nOpção inválida. Pressione ENTER para tentar novamente...")

        print("*" * 40)
        
if __name__ == '__main__':
    menu()