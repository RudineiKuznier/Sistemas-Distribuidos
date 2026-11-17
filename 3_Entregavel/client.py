import grpc
import json
import sys
import cluster_pb2
import cluster_pb2_grpc

class ClusterClient:
    def __init__(self, config, initial_node):
        self.config = config
        self.current = initial_node
        self.channels = {}

    def _stub(self, node_id):
        if node_id not in self.channels:
            self.channels[node_id] = grpc.insecure_channel(self.config[node_id])
        return cluster_pb2_grpc.NodeServiceStub(self.channels[node_id])

    def request(self, msg, redirect=True):
        last_response = None
        for attempt in range(3):
            try:
                response = self._stub(self.current).ClientRequest(msg)
            except Exception as e:
                print(f"  [erro] {self.current}: {e}")
                return None
            last_response = response

            if not redirect or response.success:
                return response

            if response.leader_id and response.leader_id != self.current:
                print(f"  [redirect] {self.current} → {response.leader_id}")
                self.current = response.leader_id
                continue

            return response
        return last_response


def run_client():
    with open("config.json") as f:
        config = json.load(f)

    initial_node = sys.argv[1]
    cli = ClusterClient(config, initial_node)

    print(f"--- Cliente conectado a {initial_node} ({config[initial_node]}) ---")
    print("Comandos: STATUS, PUT <chave> <valor>, GET <chave>, SAIR")

    while True:
        try:
            cmd = input("> ").strip().split()
            if not cmd:
                continue
            
            op = cmd[0].upper()
            if op == "SAIR":
                break

            key = cmd[1] if len(cmd) > 1 else ""
            value = cmd[2] if len(cmd) > 2 else ""

            msg = cluster_pb2.ClientRequestMsg(operation=op, key=key, value=value)
            redirect = (op == "PUT")
            response = cli.request(msg, redirect=redirect)

            if response is None:
                continue

            extra = f" | Value={response.value}" if response.value else ""
            print(f"Resposta: Success={response.success} | Msg={response.message} | Líder={response.leader_id}{extra}")

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Erro de conexão: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python client.py <node_id> (ex: node1)")
        sys.exit(1)

    run_client()