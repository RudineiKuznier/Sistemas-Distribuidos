import grpc
import json
import sys
import cluster_pb2
import cluster_pb2_grpc

def run_client():
    with open("config.json") as f:
        config = json.load(f)

    target_node_id = sys.argv[1]
    target_address = config[target_node_id]

    channel = grpc.insecure_channel(target_address)
    stub = cluster_pb2_grpc.NodeServiceStub(channel)

    print(f"--- Cliente conectando ao {target_node_id} ({target_address}) ---")
    print("Comandos disponíveis: PING, PUT <chave> <valor>, GET <chave>, SAIR")

    while True:
        try:
            cmd = input("> ").strip().split()
            if not cmd:
                continue
            
            operation = cmd[0].upper()
            if operation == "SAIR":
                break

            key = cmd[1] if len(cmd) > 1 else ""
            value = cmd[2] if len(cmd) > 2 else ""

            response = stub.ClientRequest(cluster_pb2.ClientRequestMsg(operation=operation, key=key, value=value))
            
            print(f"Resposta: Success={response.success} | Msg={response.message} | Líder={response.leader_id}")

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Erro de conexão: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python client.py <node_id> (ex: node1)")
        sys.exit(1)

    run_client()