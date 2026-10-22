# Requisitos
- Python 3
- Docker
- Node.js
- pnpm

## Instalação no Linux

**via apt**
```bash
sudo apt update && sudo apt install -y python3 python3-venv python3-pip docker.io nodejs npm
```

**via pacman**
```bash
sudo pacman -Syu --needed python python-pip docker nodejs npm
```

Ative o Docker e instale o pnpm:
```bash
sudo systemctl enable --now docker
sudo npm install -g pnpm
```

# Como executar
Execute os comandos a partir da pasta `2_Entregavel`.

1. Crie o ambiente virtual e instale as dependências do backend:
```bash
python3 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

2. Inicie o RabbitMQ. Execute estes comandos na primeira vez para criar e configurar o container:
```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
docker exec rabbitmq rabbitmqctl add_vhost my_vhost
docker exec rabbitmq rabbitmqctl set_permissions -p my_vhost guest '.*' '.*' '.*'
```

Nas próximas execuções, inicie o container já criado:
```bash
docker start rabbitmq
```

3. Abra um terminal para cada processo. No primeiro, inicie o MS Estoque:
```bash
source venv/bin/activate
python backend/ms_estoque.py
```

4. No segundo terminal, inicie o Mock de Pagamento:
```bash
source venv/bin/activate
python backend/mock_pagamento.py
```

O Mock disponibiliza o checkout em `http://localhost:5003` e envia o resultado ao webhook do MS Pagamento.

5. No terceiro terminal, inicie o MS Pagamento:
```bash
source venv/bin/activate
python backend/ms_pagamento.py
```

O MS Pagamento cria `backend/pagamentos.db`.

6. No quarto terminal, inicie o Gateway:
```bash
source venv/bin/activate
python backend/app.py
```

7. No quinto terminal, inicie o MS Entrega:
```bash
source venv/bin/activate
python backend/ms_entrega.py
```

8. No sexto terminal, instale as dependências e inicie o frontend:
```bash
cd web-app
pnpm install
pnpm dev
```

### Backend
- O MS Estoque cria `backend/estoque.db` e disponibiliza o catálogo em [http://localhost:5001/api/products](http://localhost:5001/api/products).
- O Gateway consulta esse endpoint e expõe a API em [http://localhost:5000/api/products](http://localhost:5000/api/products).
- Após a reserva, o MS Pagamento solicita uma cobrança ao Mock. O frontend abre o checkout em nova aba; o Mock envia o resultado ao webhook do MS Pagamento.
- Quando o pagamento é aprovado, o MS Entrega simula a emissão da nota fiscal e o despacho, e publica `pedido.enviado` para o Gateway notificar o frontend via SSE.
- O cancelamento fica disponível no acompanhamento enquanto o pagamento estiver pendente. O Gateway publica `pedido.excluido` e o Estoque devolve a reserva.
- Swagger: [http://127.0.0.1:5000/apidocs](http://127.0.0.1:5000/apidocs)

### Frontend
- [http://localhost:5173](http://localhost:5173)