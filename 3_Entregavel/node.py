import os
os.environ.setdefault('GRPC_VERBOSITY', 'ERROR')
os.environ.setdefault('GRPC_TRACE', '')
import grpc
import json
import sys
import time
import random
import threading
from concurrent import futures
import cluster_pb2
import cluster_pb2_grpc
from persistence import Persistence
from helpers import apply_commands_to_db, count_active_followers, last_log_idx_term

class NodeService(cluster_pb2_grpc.NodeServiceServicer):

    QUORUM = 3
    HEARTBEAT_TIMEOUT = 0.35
    PUT_TIMEOUT = 5.0
    ELECTION_RANGE = (0.4, 0.8)

    def __init__(self, node_id, config):
        self.node_id = node_id
        self.config = config
        self.persistence = Persistence(f"data/{node_id}")

        # Estado em memória
        self.current_term = 0
        self.voted_for = ""
        self.commit_index = 0
        self.log = [] # Lista de cluster_pb2.LogEntry
        self.db = {}  # Banco final (comitado)
        self.role = "SEGUIDOR"
        self.leader_id = ""
        self.lock = threading.Lock()
        self.last_heartbeat_time = time.time()
        self.election_timeout = random.uniform(*self.ELECTION_RANGE)
        self.election_in_progress = False
        self.running = True

        self.channels = {}        # cache de canais gRPC

        # Tracking de replicação
        self.followers_match = {} # {follower_id: match_index}
        self.last_fetch_from = {} # {follower_id: timestamp}
        self.cond_events = {}     # {index: threading.Event}

        self._init_persistence()

    # ----- Persistência -----

    def _init_persistence(self):
        """Recupera o estado do disco na inicialização do nó."""
        self.current_term, self.voted_for, self.commit_index = self.persistence.load_state()
        for d in self.persistence.load_log():
            self.log.append(cluster_pb2.LogEntry(
                term=d["term"], index=d["index"], command=d["command"]
            ))
        self.db = self.persistence.load_db()

    def _save_state(self):
        self.persistence.save_state(self.current_term, self.voted_for, self.commit_index)

    def _append_log(self, command):
        entry = cluster_pb2.LogEntry(term=self.current_term, index=len(self.log) + 1, command=command)
        self.log.append(entry)
        self.persistence.append_log(entry)
        return entry

    def _append_entry_from_leader(self, entry):
        self.log.append(entry)
        self.persistence.append_log(entry)

    def _truncate_log(self, new_len):
        """Trunca o log em memória e persiste no disco. Se commit_index > new_len, ajusta commit_index."""
        if new_len >= len(self.log):
            return

        old_len = len(self.log)
        self.log = self.log[:new_len]
        self.persistence.rewrite_log(self.log)
        if self.commit_index > new_len:
            self.commit_index = new_len
            self._save_state()
        print(f"[{self.node_id}] TRUNCOU log: {old_len} → {new_len} entradas")

    def _get_stub(self, target_id):
        if target_id not in self.channels:
            self.channels[target_id] = grpc.insecure_channel(self.config[target_id])
        return cluster_pb2_grpc.NodeServiceStub(self.channels[target_id])

    # ----- Transições de papel -----

    def _update_term(self, new_term):
        if new_term > self.current_term:
            was_leader = self.role == "LIDER"
            self.current_term = new_term
            self.voted_for = ""
            self.leader_id = ""
            self.role = "SEGUIDOR"
            self.last_heartbeat_time = time.time()
            self.election_timeout = random.uniform(*self.ELECTION_RANGE)
            self._save_state()
            if was_leader:
                self._on_step_down()

    def _on_step_down(self):
        print(f"[{self.node_id}] STEP DOWN: liberando PUTs pendentes")
        for event in list(self.cond_events.values()):
            event.set()
        self.cond_events.clear()
        self.followers_match.clear()
        self.last_fetch_from.clear()

    # ----- Commit & aplicação -----

    def _advance_commit(self):
        """Avança commit_index se houver quórum."""
        if self.role != "LIDER":
            return

        match = {self.node_id: len(self.log)}
        match.update(self.followers_match)
        if len(match) < self.QUORUM:
            return

        sorted_vals = sorted(match.values(), reverse=True)
        candidate = sorted_vals[self.QUORUM - 1]

        if candidate <= self.commit_index:
            return

        if self.log[candidate - 1].term != self.current_term:
            return

        old_commit = self.commit_index
        self.commit_index = candidate
        apply_commands_to_db(self.db, self.log, old_commit, candidate)
        self.persistence.save_db(self.db)
        self._save_state()

        print(f"[{self.node_id}] Commit {old_commit}→{candidate} (match={match})")

        for idx in range(old_commit + 1, candidate + 1):
            ev = self.cond_events.get(idx)
            if ev:
                ev.set()

    def _apply_committed_to_db(self, new_commit):
        """Aplica ao banco de dados local todas as entradas de log entre commit_index+1 e new_commit."""
        new_commit = min(new_commit, len(self.log))
        if new_commit <= self.commit_index:
            return
        old = self.commit_index
        apply_commands_to_db(self.db, self.log, old, new_commit)
        self.commit_index = new_commit
        self.persistence.save_db(self.db)
        self._save_state()
        print(f"[{self.node_id}] Commit {old}→{new_commit} (via líder)")

    # ----- RPCs -----

    def RequestVote(self, request, context):
        """RPC para solicitar voto em uma eleição."""
        with self.lock:
            if request.term > self.current_term:
                self._update_term(request.term)

            if request.term < self.current_term:
                return cluster_pb2.VoteResponse(term=self.current_term, vote_granted=False)

            if self.voted_for and self.voted_for != request.candidate_id:
                return cluster_pb2.VoteResponse(term=self.current_term, vote_granted=False)

            my_idx, my_term = last_log_idx_term(self.log)
            log_ok = (request.last_log_term > my_term) or \
                     (request.last_log_term == my_term and request.last_log_index >= my_idx)
            if not log_ok:
                return cluster_pb2.VoteResponse(term=self.current_term, vote_granted=False)

            self.voted_for = request.candidate_id
            self.leader_id = request.candidate_id
            self.last_heartbeat_time = time.time()
            self.election_timeout = random.uniform(*self.ELECTION_RANGE)
            self._save_state()
            print(f"[{self.node_id}] Votou em {request.candidate_id} (Termo {request.term})")
            return cluster_pb2.VoteResponse(term=self.current_term, vote_granted=True)

    def Fetch(self, request, context):
        """RPC para replicação de log e heartbeat."""
        with self.lock:
            if request.term > self.current_term:
                self._update_term(request.term)

            if request.term < self.current_term:
                return cluster_pb2.FetchResponse(term=self.current_term, success=False, leader_id=self.leader_id)

            if self.role != "LIDER":
                return cluster_pb2.FetchResponse(
                    term=self.current_term, success=False,
                    leader_id=self.leader_id if self.leader_id else ""
                )

            # Se for líder, atualiza o timestamp do último heartbeat recebido do seguidor
            self.last_fetch_from[request.follower_id] = time.time()
            L = request.last_log_index
            T = request.last_log_term

            # CASO 1: follower à frente -> trunca até o tamanho do líder
            if L > len(self.log):
                conflict = len(self.log)
                print(f"[{self.node_id}] LOG_MISMATCH {request.follower_id} à frente "
                      f"(L={L} > len={len(self.log)}) → conflict={conflict}")
                return cluster_pb2.FetchResponse(
                    term=self.current_term, success=False,
                    conflict_index=conflict,
                    leader_id=self.node_id,
                    commit_index=self.commit_index
                )

            # CASO 2: divergência de termo em L
            if L > 0 and self.log[L - 1].term != T:
                conflict = 0
                for i in range(L):
                    if self.log[i].term > T:
                        conflict = i
                        break
                print(f"[{self.node_id}] LOG_MISMATCH {request.follower_id} "
                      f"(L={L}, T={T}) vs log[L-1].term={self.log[L-1].term} "
                      f"→ conflict={conflict}")
                return cluster_pb2.FetchResponse(
                    term=self.current_term, success=False,
                    conflict_index=conflict,
                    leader_id=self.node_id,
                    commit_index=self.commit_index
                )

            # CASO 3: consistente
            self.followers_match[request.follower_id] = L
            self._advance_commit()

            return cluster_pb2.FetchResponse(
                term=self.current_term, success=True,
                leader_id=self.node_id, commit_index=self.commit_index,
                entries=self.log[L:]
            )

    def ClientRequest(self, request, context):
        """RPC para processar requisições de clientes."""
        with self.lock:
            leader = self.leader_id

        if request.operation == "STATUS":
            with self.lock:
                extra = ""
                if self.role == "LIDER":
                    active = count_active_followers(
                        self.last_fetch_from, self.node_id, self.config, self.HEARTBEAT_TIMEOUT
                    )
                    extra = f" | Ativos={1+active}/4 | Match={dict(self.followers_match)}"
                return cluster_pb2.ClientResponseMsg(
                    success=True,
                    message=f"Node={self.node_id} Papel={self.role} Termo={self.current_term} "
                            f"Commit={self.commit_index} Log={len(self.log)} Lider={self.leader_id or '?'}{extra}",
                    leader_id=self.leader_id)

        if request.operation == "GET":
            with self.lock:
                found = request.key in self.db
                value = self.db.get(request.key, "")
            return cluster_pb2.ClientResponseMsg(
                success=found, value=value,
                message=f"{request.key}={value}" if found else f"{request.key} não existe",
                leader_id=leader)

        if request.operation == "PUT":
            with self.lock:
                if self.role != "LIDER":
                    return cluster_pb2.ClientResponseMsg(
                        success=False,
                        message=f"Não sou líder. Tente: {self.leader_id or '?'}",
                        leader_id=self.leader_id)

                active = count_active_followers(
                    self.last_fetch_from, self.node_id, self.config, self.HEARTBEAT_TIMEOUT
                )
                if 1 + active < self.QUORUM:
                    return cluster_pb2.ClientResponseMsg(
                        success=False,
                        message=f"Sem quórum: {1+active}/{self.QUORUM} nós ativos. Escrita rejeitada.",
                        leader_id=self.node_id)

                entry = self._append_log(f"PUT {request.key} {request.value}")
                event = threading.Event()
                self.cond_events[entry.index] = event
                print(f"[{self.node_id}] PUT recebido: {request.key}={request.value} → idx={entry.index} (aguardando commit)")

            signaled = event.wait(timeout=self.PUT_TIMEOUT)

            with self.lock:
                self.cond_events.pop(entry.index, None)
                if not signaled:
                    return cluster_pb2.ClientResponseMsg(
                        success=False,
                        message=f"Timeout aguardando commit (idx={entry.index}). Não comitado.",
                        leader_id=self.leader_id)
                if self.commit_index >= entry.index:
                    return cluster_pb2.ClientResponseMsg(
                        success=True,
                        message=f"PUT confirmado (idx={entry.index}, commit={self.commit_index})",
                        leader_id=self.node_id)
                return cluster_pb2.ClientResponseMsg(
                    success=False,
                    message=f"Liderança perdida antes do commit (idx={entry.index})",
                    leader_id=self.leader_id or "")

        return cluster_pb2.ClientResponseMsg(success=False, message="Operação não implementada")

    # ----- Loops -----

    def _election_timer_loop(self):
        """Loop que verifica se é hora de iniciar uma eleição."""
        while self.running:
            time.sleep(0.05)
            try:
                should_start = False
                with self.lock:
                    if self.role == "LIDER" or self.election_in_progress:
                        continue
                    if (time.time() - self.last_heartbeat_time) > self.election_timeout:
                        should_start = True
                if should_start:
                    self._start_election()
            except Exception as e:
                print(f"[{self.node_id}] !!! ERRO no timer de eleição: {e}")

    def _send_vote_request(self, target_id, term):
        try:
            stub = self._get_stub(target_id)
            with self.lock:
                last_idx, last_term = last_log_idx_term(self.log)
            return stub.RequestVote(cluster_pb2.VoteRequest(
                term=term, candidate_id=self.node_id,
                last_log_index=last_idx, last_log_term=last_term
            ), timeout=0.3)
        except Exception:
            return None

    def _start_election(self):
        with self.lock:
            if self.role == "LIDER" or self.election_in_progress:
                return
            self.election_in_progress = True
            self.role = "CANDIDATO"
            self.current_term += 1
            self.voted_for = self.node_id
            self.leader_id = ""
            self.last_heartbeat_time = time.time()
            self.election_timeout = random.uniform(*self.ELECTION_RANGE)
            self._save_state()
            term = self.current_term
            print(f"[{self.node_id}] >>> Iniciando ELEIÇÃO Termo {term} <<<")

        votes = 1
        won = False
        try:
            executor = futures.ThreadPoolExecutor(max_workers=3)
            fs = [executor.submit(self._send_vote_request, nid, term)
                  for nid in self.config if nid != self.node_id]
            for fut in futures.as_completed(fs):
                if won:
                    break
                response = fut.result()
                if response is None:
                    continue
                with self.lock:
                    if self.role != "CANDIDATO" or self.current_term != term:
                        break
                    if response.term > self.current_term:
                        self._update_term(response.term)
                        break
                    if response.vote_granted:
                        votes += 1
                        if votes >= self.QUORUM:
                            self.role = "LIDER"
                            self.leader_id = self.node_id
                            self.last_heartbeat_time = time.time()
                            self._save_state()
                            print(f"[{self.node_id}] Assumiu liderança. Aguardando FetchRequests.")
                            print(f"[{self.node_id}] ### ELEITO LÍDER (Termo {term}) ###")
                            won = True
                            break
            executor.shutdown(wait=False)
        except Exception as e:
            print(f"[{self.node_id}] !!! ERRO na eleição: {e}")
        finally:
            with self.lock:
                self.election_in_progress = False
            if not won:
                print(f"[{self.node_id}] Eleição Termo {term} falhou (votos={votes})")

    def _follower_fetch_loop(self):
        """Loop que seguidores usam para descobrir o líder e receber atualizações de log."""
        while self.running:
            time.sleep(0.1)
            try:
                with self.lock:
                    if self.role != "SEGUIDOR":
                        continue
                    if self.leader_id and self.leader_id != self.node_id:
                        targets = [self.leader_id]
                    else:
                        targets = [nid for nid in self.config if nid != self.node_id]

                for target_id in targets:
                    with self.lock:
                        if self.role != "SEGUIDOR":
                            break
                        if self.leader_id and self.leader_id != target_id:
                            break
                        term = self.current_term
                        last_idx, last_term = last_log_idx_term(self.log)

                    try:
                        stub = self._get_stub(target_id)
                        response = stub.Fetch(cluster_pb2.FetchRequest(
                            term=term, follower_id=self.node_id,
                            last_log_index=last_idx, last_log_term=last_term
                        ), timeout=0.12)

                        with self.lock:
                            if response.term > self.current_term:
                                self._update_term(response.term)
                                break

                            # Follower consistente com líder
                            if response.success and response.leader_id:
                                if self.leader_id != response.leader_id:
                                    print(f"[{self.node_id}] Descobriu LÍDER: {response.leader_id}")
                                self.leader_id = response.leader_id
                                self.last_heartbeat_time = time.time()

                                # Aplica novas entradas
                                for entry in response.entries:
                                    if entry.index == len(self.log) + 1:
                                        self._append_entry_from_leader(entry)
                                    else:
                                        print(f"[{self.node_id}] ERRO: gap (esperado {len(self.log)+1}, "
                                              f"recebido {entry.index})")
                                        break

                                # Atualiza commit
                                if response.commit_index > self.commit_index:
                                    self._apply_committed_to_db(response.commit_index)
                                break

                            # LOG_MISMATCH: respondente é o próprio líder
                            elif not response.success and response.leader_id == target_id:
                                if self.leader_id != response.leader_id:
                                    print(f"[{self.node_id}] Descobriu LÍDER: {response.leader_id}")
                                self.leader_id = response.leader_id
                                self.last_heartbeat_time = time.time()

                                if response.conflict_index < len(self.log):
                                    self._truncate_log(response.conflict_index)
                                break
                    except Exception:
                        continue
            except Exception as e:
                print(f"[{self.node_id}] !!! ERRO no fetch loop: {e}")

    def start_background_threads(self):
        """Inicia os loops de eleição e fetch em threads separadas."""
        threading.Thread(target=self._election_timer_loop, daemon=True).start()
        threading.Thread(target=self._follower_fetch_loop, daemon=True).start()

def run_server(node_id):
    with open("config.json") as f:
        config = json.load(f)
    port = config[node_id].split(':')[1]

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    node_instance = NodeService(node_id, config)
    cluster_pb2_grpc.add_NodeServiceServicer_to_server(node_instance, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    node_instance.start_background_threads()
    print(f"=== Node {node_id} @ {config[node_id]} | Termo={node_instance.current_term} "
          f"Commit={node_instance.commit_index} Log={len(node_instance.log)} ===")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"\nEncerrando Node {node_id}")
        node_instance.running = False
        server.stop(0)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python node.py <node_id> (ex: node1)")
        sys.exit(1)

    run_server(sys.argv[1])