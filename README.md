# Sistemas-Distribuidos

docker exec -it <nome-do-container> rabbitmqctl add_vhost my_vhost

### 2. Gera a chave privada (2048 bits)
openssl genrsa -out keys/private_key.pem 2048

### 3. Extrai a chave pública correspondente
openssl rsa -pubout -in keys/private_key.pem -out keys/public_key.pem
