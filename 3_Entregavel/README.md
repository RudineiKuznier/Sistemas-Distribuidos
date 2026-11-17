# Replicação Pull com eleição

Sistema distribuído de replicação chave-valor com 1 cliente + 4 nós, comunicação exclusivamente via gRPC. Implementa eleição de líder estilo Raft e replicação Pull (Fetch-based, estilo KRaft/Kafka).

## Preparação do ambiente
Na pasta raiz do projeto, ative o ambiente virtual e instale as dependências:
```bash
python -m venv .venv
source .venv/bin/Activate #python .venv/Scripts/activate no Windows
python pip -m install -r requirements.txt
```

Em seguida, gere os arquivos pb2 usando protoc (é necessário repetir sempre que houver edição no cluster.proto):
```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. cluster.proto
```

## Módulos

### `cluster.proto`
Contrato gRPC. Define 3 RPCs (`RequestVote`, `Fetch`, `ClientRequest`) e as mensagens trocadas entre nós e entre cliente e nó. É a fonte única de verdade sobre o que trafega na rede.

### `node.py`
Contém a máquina de estados (SEGUIDOR -> CANDIDATO -> LÍDER), os handlers gRPC e os 2 loops em background:
- `_election_timer_loop`: dispara eleição se o líder sumir;
- `_follower_fetch_loop`: envia `Fetch` a cada 100ms ao líder; aplica novas entradas e trunca log divergente.

Também concentra o avanço de commit por quórum (`_advance_commit`), o bloqueio de escrita até commit (`_handle_put`) e o tratamento de redirecionamento para o líder.

### `persistence.py`
Camada de disco. Classe `Persistence` com operações de leitura/escrita sobre 3 arquivos por nó:
- `state.json`: termo atual, voto concedido, commit_index.
- `log.jsonl`: entradas de log (append; reescrito só em truncamento).
- `db.json`: dados comitados (chave-valor).

### `helpers.py`
Funções puras reutilizadas pelo `node.py`:
- `apply_commands_to_db`: aplica ao db as entradas comitadas cujos índices são maiores que o commit index antigo e menores ou iguais ao novo commit index.
- `count_active_followers`: quantos seguidores enviaram Fetch nos últimos `timeout` segundos.
- `last_log_idx_term`: retorna `(último índice, último termo)` do log.

### `client.py`
CLI que se conecta a um nó e traduz comandos do usuário em `ClientRequest`. Classe `ClusterClient` implementa **redirecionamento automático**: quando um nó seguidor informa quem é o líder, o cliente reenvia a solicitação diretamente ao líder.

### `config.json`
Mapeia `node_id -> host:porta`. Lido por todos os processos na inicialização para saber os endereços dos peers.

## Como o gRPC conecta o fluxo

Três chamadas gRPC sustentam os pilares do consenso:

**1. `RequestVote` — eleição.**
Seguidor vira candidato, incrementa seu termo e envia `RequestVote(term, last_log_index, last_log_term)` em paralelo aos pares. Cada par valida: (a) termo do solicitante ≥ seu, (b) ainda não votou neste termo, (c) log do candidato é tão ou mais atualizado. Ao atingir 3 votos (quórum), vira líder. Se em qualquer resposta chegar termo maior, vira seguidor.

**2. `Fetch` — replicação e heartbeat.**
Seguidores enviam `Fetch(term, last_log_index, last_log_term)` ao líder a cada 100ms. O líder responde de três formas:
- `success=true` + `entries` faltantes + `commit_index` → seguidor aplica e atualiza commit.
- `success=false` + `conflict_index` → seguidor trunca seu log até esse índice e tenta novamente.
- `term` maior no payload → seguidor aplica atualização obrigatória de termo.

O `FetchRequest` também serve de heartbeat: o líder marca o timestamp de cada recebimento em `last_fetch_from[follower_id]`. Se algum seguidor parar de enviar por 350ms, o líder perde quórum e rejeita novas escritas.

**3. `ClientRequest` — operações do cliente.**
`GET` lê apenas do db (dados comitados — garante consistência eventual isolando uncommitted). `PUT` no líder insere no log, bloqueia em `threading.Event` até o commit avançar (com timeout de 5s) e responde. `PUT` em seguidor retorna `success=false` + `leader_id`, permitindo que o cliente redirecione automaticamente. `STATUS` retorna papel, termo, commit e (no líder) seguidores ativos.

## Parâmetros de operação (`node.py`)

- `QUORUM = 3` — maioria absoluta de 4 nós.
- `HEARTBEAT_TIMEOUT = 0.35s` — seguidor inativo após esse silêncio.
- `PUT_TIMEOUT = 5.0s` — bloqueio máximo do cliente aguardando commit.
- `ELECTION_RANGE = (0.4, 0.8)s` — intervalo aleatório para evitar empates na votação.
- Loop de Fetch: 100ms.

## Persistência por nó

Cada nó grava em `data/{node_id}/`:
- `state.json`: `{current_term, voted_for, commit_index}`.
- `log.jsonl`: uma linha por entrada `{term, index, command}`.
- `db.json`: dicionário `{chave: valor}` dos dados comitados.

Ao reiniciar, o nó reconstrói seu estado em memória a partir desses arquivos, garantindo resistência a quedas.