import time

class NodeStatusManager:
    def __init__(self):
        self.last_seen = {}

    def update(self, node_id):
        self.last_seen[node_id] = time.time()

    def is_alive(self, node_id, timeout=5):
        last = self.last_seen.get(node_id)
        return last is not None and (time.time() - last < timeout)

    def get_all_statuses(self, timeout=5):
        now = time.time()
        return {
            node_id: ("ALIVE" if now - ts < timeout else "DEAD")
            for node_id, ts in self.last_seen.items()
        }

# Singleton instance để module khác import dùng chung
node_status_manager = NodeStatusManager()