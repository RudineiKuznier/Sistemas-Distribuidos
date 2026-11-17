import time

def apply_commands_to_db(db, log, old_commit, new_commit):
    """Aplica ao dict `db` todas as entradas de `log` entre old_commit+1 e new_commit."""
    for idx in range(old_commit + 1, new_commit + 1):
        parts = log[idx - 1].command.split()
        if len(parts) >= 3 and parts[0] == "PUT":
            db[parts[1]] = parts[2]

def count_active_followers(last_fetch_from, node_id, config, timeout):
    """Quantos seguidores enviaram Fetch nos últimos `timeout` segundos."""
    now = time.time()
    return sum(
        1 for f in config
        if f != node_id and now - last_fetch_from.get(f, 0) < timeout
    )

def last_log_idx_term(log):
    """(último índice, último termo) do log. (0, 0) se vazio."""
    if not log:
        return 0, 0
    return log[-1].index, log[-1].term