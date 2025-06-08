from router_node import get_responsible_nodes, forward_request
from config import NODE_PORTS as ALL_KV_NODE_PORTS
from node_status_manager import node_status_manager
from config import STATUS_OK, STATUS_ERROR, STATUS_NOT_FOUND


class KVNodeLogic:
    def __init__(self, kvstore, port, log_func):
        self.kv = kvstore
        self.port = port
        self.log = log_func

    async def sync_missing_data(self):
        all_keys = set(self.kv.store.keys())
        self.log(f"[{self.port}] Local store keys at start: {list(all_keys)}")

        for other_port in ALL_KV_NODE_PORTS:
            if other_port == self.port or not node_status_manager.is_alive(other_port):
                continue
            try:
                response = await forward_request(other_port, {
                    "action": "list_keys"
                })
                if response["status"] == STATUS_OK:
                    self.log(f"[{self.port}] Got keys from node {other_port}: {response['keys']}")
                    all_keys.update(response["keys"])
            except Exception as e:
                self.log(f"[{self.port}] Could not fetch keys from node {other_port}: {e}")

        self.log(f"[{self.port}] Total keys to check: {sorted(all_keys)}")

        for key in sorted(all_keys):
            responsible_nodes = get_responsible_nodes(key)
            self.log(f"[{self.port}] Checking key '{key}', responsible nodes: {responsible_nodes}")
            if self.port not in responsible_nodes:
                self.log(f"[{self.port}] Skipping key '{key}': not responsible")
                continue

            local_data = self.kv.store.get(key)
            local_version = local_data.get("version", 0) if local_data else 0
            local_deleted = local_data.get("deleted", False) if local_data else False

            for other_port in responsible_nodes:
                if other_port == self.port or not node_status_manager.is_alive(other_port):
                    continue
                try:
                    response = await forward_request(other_port, {
                        "action": "get",
                        "key": key,
                        "internal": True
                    })
                    if response["status"] != STATUS_OK:
                        self.log(f"[{self.port}] No data for key '{key}' from node {other_port}")
                        continue

                    remote_data = response["value"]
                    remote_version = remote_data.get("version", 0)
                    remote_deleted = remote_data.get("deleted", False)

                    self.log(f"[{self.port}] Compare key '{key}' from node {other_port}: "
                             f"local_v={local_version}, local_del={local_deleted} | "
                             f"remote_v={remote_version}, remote_del={remote_deleted}")

                    if remote_version > local_version or (
                        remote_version == local_version and remote_deleted and not local_deleted
                    ):
                        self.kv.store[key] = remote_data
                        self.kv.save_store()
                        self.log(f"[{self.port}] Updated key '{key}' to version {remote_version} (deleted={remote_deleted})")
                    else:
                        self.log(f"[{self.port}] Kept local key '{key}' version {local_version} (deleted={local_deleted})")

                except Exception as e:
                    self.log(f"[{self.port}] Sync error for key '{key}' from node {other_port}: {e}")

    async def act_as_temporary_primary(self, key, value=None, is_delete=False):
        if is_delete:
            current_version = self.kv.store.get(key, {}).get("version", 0) + 1
            self.kv.store[key] = {
                "value": None,
                "version": current_version,
                "deleted": True
            }
            self.kv.save_store()
        else:
            existed = key in self.kv.store
            version = self.kv.store[key]["version"] + 1 if existed else 1
            self.kv.store[key] = {
                "value": value,
                "version": version,
                "deleted": False
            }
            self.kv.save_store()

        for replica_port in get_responsible_nodes(key):
            if replica_port == self.port or not node_status_manager.is_alive(replica_port):
                continue
            try:
                await forward_request(replica_port, {
                    "action": "replica_delete" if is_delete else "replica_put",
                    "key": key,
                    "value": value if not is_delete else None,
                    "version": current_version if is_delete else version
                })
            except Exception as e:
                self.log(f"[{self.port}] [Fallback] Failed to contact replica {replica_port}: {e}")

        return {
            "status": STATUS_OK,
            "message": f"[Fallback] {'Deleted' if is_delete else 'Stored'} {key}"
        }

    async def handle(self, cmd):
        action = cmd.get("action", "").lower()
        key = cmd.get("key")
        value = cmd.get("value")

        if action == "list_keys":
            key_list = list(self.kv.store.keys())
            self.log(f"[{self.port}] list_keys includes: {key_list}")
            return {"status": STATUS_OK, "keys": key_list}

        if not action or not key:
            return {"status": STATUS_ERROR, "message": "Missing action or key"}

        nodes = get_responsible_nodes(key)

        if action == "replica_put":
            incoming_version = cmd.get("version", 1)
            self.log(f"[{self.port}] replica_put received value = {value}, version = {incoming_version}")
            local_data = self.kv.store.get(key)

            if not local_data or incoming_version > local_data.get("version", 0):
                self.kv.store[key] = {
                    "value": value,
                    "version": incoming_version,
                    "deleted": False
                }
                self.kv.save_store()
                return {"status": STATUS_OK, "message": "Replicated"}
            return {"status": STATUS_OK, "message": "Ignored older version"}

        if action == "replica_delete":
            incoming_version = cmd.get("version", 1)
            local_version = self.kv.store.get(key, {}).get("version", 0)

            if incoming_version > local_version:
                self.kv.store[key] = {
                    "value": None,
                    "version": incoming_version,
                    "deleted": True
                }
                self.kv.save_store()
                return {"status": STATUS_OK, "message": "Replica tombstone written"}
            return {"status": STATUS_OK, "message": "Ignored older delete version"}

        if action == "put":
            primary = nodes[0]

            if self.port == primary:
                existed = key in self.kv.store
                version = self.kv.store[key]["version"] + 1 if existed else 1
                self.kv.store[key] = {
                    "value": value,
                    "version": version,
                    "deleted": False
                }
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

            if cmd.get("forwarded") or not node_status_manager.is_alive(primary):
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

        if action == "get":
            internal = cmd.get("internal", False)
            if key in self.kv.store:
                record = self.kv.store[key]
                if record.get("deleted", False) and not internal:
                    return {"status": STATUS_NOT_FOUND, "message": f"Key '{key}' not found (deleted)"}
                return {"status": STATUS_OK, "value": record}
            for node_port in nodes:
                if node_port == self.port or not node_status_manager.is_alive(node_port):
                    continue
                try:
                    cmd["internal"] = True
                    response = await forward_request(node_port, cmd)
                    if response.get("status") == STATUS_OK:
                        return response
                except Exception as e:
                    self.log(f"[{self.port}] GET forward to {node_port} failed: {e}")
            return {"status": STATUS_NOT_FOUND, "message": f"Key '{key}' not found"}

        if action == "delete":
            primary = nodes[0]

            if self.port == primary:
                current_version = self.kv.store.get(key, {}).get("version", 0) + 1
                self.kv.store[key] = {
                    "value": None,
                    "version": current_version,
                    "deleted": True
                }
                self.kv.save_store()

                for replica_port in nodes[1:]:
                    try:
                        await forward_request(replica_port, {
                            "action": "replica_delete",
                            "key": key,
                            "version": current_version
                        })
                    except Exception as e:
                        self.log(f"[{self.port}] Replica DELETE failed to {replica_port}: {e}")

                return {
                    "status": STATUS_OK,
                    "message": f"Deleted {key}"
                }

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

        return {"status": STATUS_ERROR, "message": f"Unknown action: {action}"}