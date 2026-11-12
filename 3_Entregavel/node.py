import grpc
import json
import sys
import os
from concurrent import futures
import cluster_pb2
import cluster_pb2_grpc

class NodeService(cluster_pb2_grpc.NodeServiceServicer):
    def __init__(self, node_id):
        self.node_id = node_id
        self.data_dir = f"data/{node_id}"
        
        # Estado em memória
        self.current_term = 0
        self.voted_for = ""
        self.commit_index = 0
        self.log = [] # Lista de cluster_pb2.LogEntry
        self.db = {}  # Banco final (comitado)
        self.role = "SEGUIDOR"

        self._init_persistence()

    def _init_persistence(self):
        """Recupera o estado do disco na inicialização do nó."""
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Carregar estado (Termo, Voto, Commit)
        state_file = os.path.join(self.data_dir, "state.json")
        if os.path.exists(state_file):
            with open(state_file, "r") as f:
                state = json.load(f)
                self.current_term = state.get("current_term", 0)
                self.voted_for = state.get("voted_for", "")
                self.commit_index = state.get("commit_index", 0)
        
        # Carregar log
        log_file = os.path.join(self.data_dir, "log.jsonl")
        if os.path.exists(log_file):
            with open(log_file, "r") as f:
                for line in f:
                    if line.strip():
                        data = json.loads(line)
                        self.log.append(cluster_pb2.LogEntry(
                            term=data["term"], index=data["index"], command=data["command"]
                        ))
        
        # Carregar banco
        db_file = os.path.join(self.data_dir, "db.json")
        if os.path.exists(db_file):
            with open(db_file, "r") as f:
                self.db = json.load(f)

    def _save_state(self):
        """Salva o estado no disco."""
        state_file = os.path.join(self.data_dir, "state.json")
        with open(state_file, "w") as f:
            json.dump({
                "current_term": self.current_term,
                "voted_for": self.voted_for,
                "commit_index": self.commit_index
            }, f)

    def _append_log(self, command):
        """Adiciona uma nova entrada no log local."""
        new_index = len(self.log) + 1
        entry = cluster_pb2.LogEntry(term=self.current_term, index=new_index, command=command)
        self.log.append(entry)
        
        log_file = os.path.join(self.data_dir, "log.jsonl")
        with open(log_file, "a") as f:
            f.write(json.dumps({"term": entry.term, "index": entry.index, "command": entry.command}) + "\n")
        return entry

    def _apply_to_db(self, command):
        """Aplica um comando comitado ao banco final."""
        parts = command.split()
        if len(parts) >= 3 and parts[0] == "PUT":
            self.db[parts[1]] = parts[2]
        
        db_file = os.path.join(self.data_dir, "db.json")
        with open(db_file, "w") as f:
            json.dump(self.db, f)

    # ---------------- RPCs ----------------
    
    def RequestVote(self, request, context):
        if request.term > self.current_term:
            self.current_term = request.term
            self.role = "SEGUIDOR"
            self.voted_for = "" # Reseta voto para o novo termo
            self._save_state()
            
        print(f"[{self.node_id}] Recebeu RequestVote de {request.candidate_id} (TermoReq: {request.term})")
        return cluster_pb2.VoteResponse(term=self.current_term, vote_granted=False)

    def Fetch(self, request, context):
        if request.term > self.current_term:
            self.current_term = request.term
            self.role = "SEGUIDOR"
            self.voted_for = ""
            self._save_state()
            
        print(f"[{self.node_id}] Recebeu Fetch de {request.follower_id} (TermoReq: {request.term})")
        return cluster_pb2.FetchResponse(term=self.current_term, success=False)

    def ClientRequest(self, request, context):
        print(f"[{self.node_id}] Recebeu ClientRequest: {request.operation} {request.key} {request.value}")
        
        if request.operation == "PING":
            return cluster_pb2.ClientResponseMsg(success=True, message=f"PONG do {self.node_id}")

        # COMANDO DE TESTE DA ETAPA 2 (Apenas grava no log)
        if request.operation == "PUT":
            cmd = f"PUT {request.key} {request.value}"
            entry = self._append_log(cmd)
            return cluster_pb2.ClientResponseMsg(success=True, message=f"Log adicionado: Index={entry.index}, Term={entry.term}. (Aguardando commit)")

        # COMANDO DE TESTE DA ETAPA 2 (Força o commit do último log para testar DB)
        if request.operation == "TEST_COMMIT":
            if len(self.log) > 0:
                last_entry = self.log[-1]
                self.commit_index = last_entry.index
                self._apply_to_db(last_entry.command)
                self._save_state()
                return cluster_pb2.ClientResponseMsg(success=True, message=f"Comitado até índice {self.commit_index}. Banco atualizado: {self.db}")
            return cluster_pb2.ClientResponseMsg(success=False, message="Log vazio.")
        
        return cluster_pb2.ClientResponseMsg(success=False, message="Operação ainda não implementada")

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