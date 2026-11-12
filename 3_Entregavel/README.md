# gRPC

## Preparação do ambiente
Na pasta raiz do projeto, execute:
```bash
python -m venv .venv
source .venv/bin/Activate #python .venv/Scripts/activate no Windows
python pip -m install -r requirements.txt
```

Em seguida, gere os arquivos pb2 usando protoc (é necessário repetir sempre que houver edição no cluster.proto):
```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. cluster.proto
```