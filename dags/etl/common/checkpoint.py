import os, json

class CheckpointManager:
    def __init__(self, folder="/opt/airflow/checkpoints"):
        self.folder = folder
        os.makedirs(folder, exist_ok=True)

    def load(self, name):
        p = f"{self.folder}/{name}.json"
        return int(json.load(open(p)).get("last_ts", 0)) if os.path.exists(p) else 0

    def save(self, name, ts):
        with open(f"{self.folder}/{name}.json", "w") as f:
            json.dump({"last_ts": ts}, f)