import grpc
import json
import sys
from concurrent import futures
import cluster_pb2
import cluster_pb2_grpc

class NodeService(cluster_pb2_grpc.NodeServiceServicer):
    def __init__(self, node_id):
        self.node_id = node_id
        self.current_term = 0
        self.role = "SEGUIDOR"
        self.commit_index = 0

    def RequestVote(self, request, context):
        if request.term > self.current_term:
            self.current_term = request.term
            self.role = "SEGUIDOR"

        print(f"[{self.node_id}] Recebeu RequestVote de {request.candidate_id} (TermoReq: {request.term})")
        # Retorna o termo LOCAL, não o do solicitante
        return cluster_pb2.VoteResponse(term=self.current_term, vote_granted=False)

    def Fetch(self, request, context):
        if request.term > self.current_term:
            self.current_term = request.term
            self.role = "SEGUIDOR"
        
        print(f"[{self.node_id}] Recebeu Fetch de {request.follower_id} (TermoReq: {request.term})")
        return cluster_pb2.FetchResponse(term=self.current_term, success=False)

    def ClientRequest(self, request, context):
        print(f"[{self.node_id}] Recebeu ClientRequest: {request.operation} {request.key} {request.value}")
        
        # Tratamento básico para testar o CLI
        if request.operation == "PING":
            return cluster_pb2.ClientResponseMsg(success=True, message=f"PONG do {self.node_id}")
        
        return cluster_pb2.ClientResponseMsg(
            success=False, 
            message="Operação ainda não implementada",
            leader_id=""
        )

def run_server(node_id):
    with open("config.json") as f:
        config = json.load(f)
    port = config[node_id].split(':')[1]
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    node_instance = NodeService(node_id)
    cluster_pb2_grpc.add_NodeServiceServicer_to_server(node_instance, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    
    print(f"Node {node_id} rodando na porta {port} | Papel: {node_instance.role} | Termo: {node_instance.current_term} | Commit: {node_instance.commit_index}")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"Encerrando Node {node_id}")
        server.stop(0)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python node.py <node_id> (ex: node1)")
        sys.exit(1)

    run_server(sys.argv[1])