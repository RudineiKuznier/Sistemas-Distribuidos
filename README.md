# Sistemas Distribuídos

## Ambiente

### RabbitMQ com Docker

Com o Docker instalado e em execução, baixe a imagem oficial do RabbitMQ com a interface de gerenciamento:

```powershell
docker pull rabbitmq:4-management
```

Crie e inicie o container, expondo a porta AMQP `5672` e a interface de
gerenciamento na porta `15672`:

```powershell
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

Crie o virtual host usado pelos microsserviços:

```powershell
docker exec -it rabbitmq rabbitmqctl add_vhost my_vhost
```

Conceda ao usuário padrão `guest` permissão para usar esse virtual host:

```powershell
docker exec -it rabbitmq rabbitmqctl set_permissions -p my_vhost guest ".*" ".*" ".*"
```

A interface de gerenciamento pode ser acessada em localhost:15672 com usuário **guest** e senha **guest**.

Se o container já tiver sido criado anteriormente, use apenas:

```powershell
docker start rabbitmq
```

### Dependências Python

Instale as dependências Python no ambiente virtual:

```powershell
pip install -r 1_Entregavel\requirements.txt
```

## Microsserviços

Os comandos abaixo devem ser executados a partir da pasta `1_Entregavel`.
Cada processo deve ser executado em um terminal separado.

### Principal

```powershell
python ms_principal.py
```

Permite criar, excluir e consultar status dos pedidos.

### Estoque

```powershell
python ms_estoque.py
```

Consome `pedido.criado` e `pedido.excluido`, reserva ou devolve produtos e publica os eventos de estoque correspondentes.

### Pagamento

```powershell
python ms_pagamento.py
```

Consome `pedido.estoque_ok` e publica uma aprovação ou recusa aleatória.

### Entrega

```powershell
python ms_entrega.py
```

Consome `pagamento.aprovado` e publica `pedido.enviado`.

### Promoções

```powershell
python ms_promocoes.py
```

Publica uma promoção aleatória nas categorias A, B ou C a cada 2 segundos.

Para publicar apenas uma categoria, informe a routing key e, opcionalmente, o intervalo em segundos:

```powershell
python ms_promocoes.py promocao.categoria.A 5
```

## Consumidores de Promoções

O arquivo `receive_promocoes_topic.py` é usado para executar C1 e C2. Cada execução cria sua própria fila exclusiva.

### C1: categorias A e B

```powershell
python receive_promocoes_topic.py promocao.categoria.A promocao.categoria.B
```

### C2: todas as categorias

```powershell
python receive_promocoes_topic.py "#"
```
