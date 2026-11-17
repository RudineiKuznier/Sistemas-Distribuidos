import os
import json

class Persistence:
    """Camada de persistência sobre os arquivos de estado em disco (state, log, db)."""

    def __init__(self, data_dir):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.state_file = os.path.join(self.data_dir, "state.json")
        self.log_file = os.path.join(self.data_dir, "log.jsonl")
        self.db_file = os.path.join(self.data_dir, "db.json")

    # ----- Carga -----

    def load_state(self):
        """Retorna (current_term, voted_for, commit_index)."""
        if not os.path.exists(self.state_file):
            return 0, "", 0
        with open(self.state_file, "r") as f:
            s = json.load(f)
        return s.get("current_term", 0), s.get("voted_for", ""), s.get("commit_index", 0)

    def load_log(self):
        """Retorna lista de dicts {term, index, command}."""
        if not os.path.exists(self.log_file):
            return []
        entries = []
        with open(self.log_file, "r") as f:
            for line in f:
                if line.strip():
                    entries.append(json.loads(line))
        return entries

    def load_db(self):
        if not os.path.exists(self.db_file):
            return {}
        with open(self.db_file, "r") as f:
            return json.load(f)

    # ----- Persistência -----

    def save_state(self, current_term, voted_for, commit_index):
        with open(self.state_file, "w") as f:
            json.dump({
                "current_term": current_term,
                "voted_for": voted_for,
                "commit_index": commit_index,
            }, f)

    def append_log(self, entry):
        with open(self.log_file, "a") as f:
            f.write(json.dumps({
                "term": entry.term,
                "index": entry.index,
                "command": entry.command,
            }) + "\n")

    def rewrite_log(self, entries):
        with open(self.log_file, "w") as f:
            for entry in entries:
                f.write(json.dumps({
                    "term": entry.term,
                    "index": entry.index,
                    "command": entry.command,
                }) + "\n")

    def save_db(self, db):
        with open(self.db_file, "w") as f:
            json.dump(db, f)