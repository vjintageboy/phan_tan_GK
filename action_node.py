from router_node import get_responsible_nodes, forward_request
from config import *
from node_status_manager import node_status_manager


class KVNodeLogic:
    def __init__(self, kvstore, port, log_func):
        self.kv = kvstore
        self.port = port
        self.log = log_func
    
    async def sync_missing_data(self):
        for key, local_data in self.kv.store.items():
            responsible_nodes = get_responsible_nodes(key)
            if self.port not in responsible_nodes:
                continue

            for other_port in responsible_nodes:
                if other_port == self.port or not node_status_manager.is_alive(other_port):
                    continue
                try:
                    response = await forward_request(other_port, {
                        "action": "get",
                        "key": key
                    })
                    if response["status"] == STATUS_OK:
                        remote_data = response["value"]
                        if remote_data["version"] > local_data["version"]:
                            self.log(f"[{self.port}] Sync: updating key '{key}' to newer version {remote_data['version']}")
                            self.kv.store[key] = remote_data
                            self.kv.save_store()
                except Exception as e:
                    self.log(f"[{self.port}] Sync error with node {other_port}: {e}")


    async def act_as_temporary_primary(self, key, value=None, is_delete=False):
        deleted = False
        if is_delete:
            deleted = key in self.kv.store
            if deleted:
                del self.kv.store[key]
                self.kv.save_store()
        else:
            existed = key in self.kv.store
            version = self.kv.store[key]["version"] + 1 if existed else 1

            # Lưu đúng định dạng
            self.kv.store[key] = {
                "value": value,
                "version": version
            }
            self.kv.save_store()

        # Gửi tới replica (PUT hoặc DELETE)
        for replica_port in get_responsible_nodes(key):
            if replica_port == self.port or not node_status_manager.is_alive(replica_port):
                continue
            try:
                await forward_request(replica_port, {
                    "action": "replica_delete" if is_delete else "replica_put",
                    "key": key,
                    "value": value if not is_delete else None,
                    "version": version if not is_delete else None
                })
            except Exception as e:
                self.log(f"[{self.port}] [Fallback] Failed to contact replica {replica_port}: {e}")

        return {
            "status": STATUS_OK if (deleted if is_delete else True) else STATUS_NOT_FOUND,
            "message": f"[Fallback] {'Deleted' if is_delete else 'Stored'} {key}"
        }


    async def handle(self, cmd):
        action = cmd.get("action", "").lower()
        key = cmd.get("key")
        value = cmd.get("value")

        if not action or not key:
            return {"status": STATUS_ERROR, "message": "Missing action or key"}

        nodes = get_responsible_nodes(key)

        # -------------------- Replica PUT --------------------
        if action == "replica_put":
            incoming_version = cmd.get("version", 1)
            self.log(f"[{self.port}] replica_put received value = {value}, version = {incoming_version}")
            local_data = self.kv.store.get(key)

            if not local_data or incoming_version > local_data["version"]:
                self.kv.store[key] = {"value": value, "version": incoming_version}
                self.kv.save_store()
                return {"status": STATUS_OK, "message": "Replicated"}
            return {"status": STATUS_OK, "message": "Ignored older version"}

        # -------------------- Replica DELETE --------------------
        if action == "replica_delete":
            if key in self.kv.store:
                del self.kv.store[key]
                self.kv.save_store()
                return {"status": STATUS_OK, "message": "Replica deleted"}
            return {"status": STATUS_NOT_FOUND, "message": "Key not found in replica"}

        # -------------------- PUT (Primary or Forwarded) --------------------
        if action == "put":
            primary = nodes[0]

            if self.port == primary:
                # This node is primary
                existed = key in self.kv.store
                version = self.kv.store[key]["version"] + 1 if existed else 1

                self.kv.store[key] = {"value": value, "version": version}
                self.kv.save_store()

                for replica_port in nodes[1:]:
                    try:
                        await forward_request(replica_port, {
                            "action": "replica_put",
                            "key": key,
                            "value": value,
                            "version": version
                        })
                    except Exception as e:
                        self.log(f"[{self.port}] Replica PUT failed to {replica_port}: {e}")

                return {"status": STATUS_OK, "message": f"{'Updated' if existed else 'Stored'} {key}"}

            # Not primary → Try forward to primary
            if cmd.get("forwarded") or not node_status_manager.is_alive(primary):
                # Already forwarded or primary is DEAD → fallback
                self.log(f"[{self.port}] PUT forward skipped or failed. Acting as fallback.")
                return await self.act_as_temporary_primary(key, value=value)
            else:
                self.log(f"[{self.port}] Forwarding PUT to primary {primary}")
                cmd["forwarded"] = True
                try:
                    response = await forward_request(primary, cmd)
                    return response
                except Exception as e:
                    self.log(f"[{self.port}] PUT forward error: {e}. Acting as fallback.")
                    return await self.act_as_temporary_primary(key, value=value)

        # -------------------- GET --------------------
        if action == "get":
            if key in self.kv.store:
                return {"status": STATUS_OK, "value": self.kv.store[key]}
            for node_port in nodes:
                if node_port == self.port or not node_status_manager.is_alive(node_port):
                    continue
                try:
                    response = await forward_request(node_port, cmd)
                    if response.get("status") == STATUS_OK:
                        return response
                except Exception as e:
                    self.log(f"[{self.port}] GET forward to {node_port} failed: {e}")
            return {"status": STATUS_NOT_FOUND, "message": f"Key '{key}' not found"}

        # -------------------- DELETE --------------------
        if action == "delete":
            primary = nodes[0]

            if self.port == primary:
                deleted = key in self.kv.store
                if deleted:
                    del self.kv.store[key]
                    self.kv.save_store()

                for replica_port in nodes[1:]:
                    try:
                        await forward_request(replica_port, {
                            "action": "replica_delete",
                            "key": key
                        })
                    except Exception as e:
                        self.log(f"[{self.port}] Replica DELETE failed to {replica_port}: {e}")

                return {
                    "status": STATUS_OK if deleted else STATUS_NOT_FOUND,
                    "message": f"{'Deleted' if deleted else 'Key not found'} {key}"
                }

            # Not primary → check alive then forward or fallback
            if not node_status_manager.is_alive(primary):
                self.log(f"[{self.port}] DELETE forward skipped. Primary {primary} is DEAD. Acting as fallback.")
                return await self.act_as_temporary_primary(key, is_delete=True)

            try:
                self.log(f"[{self.port}] Forwarding DELETE to primary {primary}")
                response = await forward_request(primary, cmd)
                return response
            except Exception as e:
                self.log(f"[{self.port}] DELETE forward failed: {e}. Acting as fallback.")
                return await self.act_as_temporary_primary(key, is_delete=True)

        # -------------------- Unknown action --------------------
        return {"status": STATUS_ERROR, "message": f"Unknown action: {action}"}
