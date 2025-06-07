import os
import json

class KVStore:
    def __init__(self, store_file):
        self.store_file = store_file
        self.store = self.load_store()

    def load_store(self):
        if os.path.exists(self.store_file):
            try:
                with open(self.store_file, "r") as f:
                    content = f.read().strip()
                    if not content:
                        print(f"[Store] Warning: Empty store file at {self.store_file}")
                        return {}
                    return json.loads(content)
            except Exception as e:
                print(f"[Store] Warning: Failed to load store file {self.store_file}: {e}")
                return {}
        return {}

    def save_store(self):
        with open(self.store_file, "w") as f:
            json.dump(self.store, f, indent=2)

    def put(self, key, value):
        if key in self.store:
            current_version = self.store[key]["version"] + 1
        else:
            current_version = 1

        self.store[key] = {
            "value": value,
            "version": current_version
        }
        self.save_store()
        return current_version

    def get(self, key):
        if key in self.store:
            return self.store[key]["value"]
        return None

    def get_with_version(self, key):
        """Trả về cả value và version, dùng khi sync/replica."""
        return self.store.get(key, None)

    def delete(self, key):
        if key in self.store:
            del self.store[key]
            self.save_store()
            return True
        return False

    def replica_put(self, key, value, version):
        current_version = self.store.get(key, {}).get("version", 0)
        if version > current_version:
            self.store[key] = {
                "value": value,
                "version": version
            }
            self.save_store()
            return True
        return False

    def replica_delete(self, key, version):
        current_version = self.store.get(key, {}).get("version", 0)
        if version >= current_version:
            if key in self.store:
                del self.store[key]
                self.save_store()
            return True
        return False
