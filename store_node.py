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
        existed = key in self.store
        self.store[key] = value
        self.save_store()
        return existed

    def get(self, key):
        existed = key in self.store
        if not existed:
            print(f"[Store] Warning: Key '{key}' not found")    
            return None
        return self.store.get(key)

    def delete(self, key):
        existed = key in self.store
        if existed:
            del self.store[key]
            self.save_store()
        return existed
