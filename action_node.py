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
        self.log(f"[{self.port}] Sync started. Local keys: {list(all_keys)}")

        for other_port in ALL_KV_NODE_PORTS:
            if other_port == self.port or not node_status_manager.is_alive(other_port):
                continue
            try:
                response = await forward_request(other_port, {"action": "list_keys"})
                if response["status"] == STATUS_OK:
                    all_keys.update(response["keys"])
            except Exception:
                pass

        for key in sorted(all_keys):
            responsible_nodes = get_responsible_nodes(key)
            if self.port not in responsible_nodes:
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
                        continue

                    remote_data = response["value"]
                    remote_version = remote_data.get("version", 0)
                    remote_deleted = remote_data.get("deleted", False)

                    if remote_version > local_version or (
                        remote_version == local_version and remote_deleted and not local_deleted
                    ):
                        self.kv.store[key] = remote_data
                        self.kv.save_store()
                        self.log(f"[{self.port}] Synced key '{key}' to version {remote_version} (deleted={remote_deleted})")
                except Exception:
                    pass

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
            except Exception:
                pass

        return {"status": STATUS_OK, "message": f"[Fallback] {'Deleted' if is_delete else 'Stored'} {key}"}

    async def handle(self, cmd):
        action = cmd.get("action", "").lower()
        key = cmd.get("key")
        value = cmd.get("value")
        
        # --- BEGIN: Thêm code mới ---
        # Action nội bộ để GUI lấy trạng thái của tất cả node
        if action == "get_status":
            statuses = node_status_manager.get_all_statuses()
            statuses[self.port] = "ALIVE"
            return {"status": STATUS_OK, "data": statuses}

        # Action nội bộ để GUI lấy toàn bộ dữ liệu của node này
        if action == "get_all_data":
            return {"status": STATUS_OK, "data": self.kv.store}
        # --- END: Thêm code mới ---

        if action == "list_keys":
            return {"status": STATUS_OK, "keys": list(self.kv.store.keys())}

        if not action or not key:
            return {"status": STATUS_ERROR, "message": "Missing action or key"}

        nodes = get_responsible_nodes(key)

        if action == "replica_put":
            incoming_version = cmd.get("version", 1)
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
                    except Exception:
                        pass
                return {"status": STATUS_OK, "message": f"{'Updated' if existed else 'Stored'} {key}"}

            if cmd.get("forwarded") or not node_status_manager.is_alive(primary):
                return await self.act_as_temporary_primary(key, value=value)
            else:
                cmd["forwarded"] = True
                try:
                    return await forward_request(primary, cmd)
                except Exception:
                    return await self.act_as_temporary_primary(key, value=value)

        if action == "get":
            internal = cmd.get("internal", False)
            if key in self.kv.store:
                record = self.kv.store[key]
                if record.get("deleted", False) and not internal:
                    return {"status": STATUS_NOT_FOUND, "message": f"Key '{key}' not found (deleted)"}
                return {"status": STATUS_OK, "value": record}

            # --- SỬA LỖI TẠI ĐÂY ---
            # Nếu đây là một yêu cầu nội bộ (đã được forward từ node khác)
            # và key không có ở đây, thì dừng lại ngay.
            if internal:
                return {"status": STATUS_NOT_FOUND, "message": f"Key not found on peer node {self.port}"}
            # --- KẾT THÚC SỬA LỖI ---

            # Logic forward này giờ chỉ chạy cho yêu cầu ban đầu từ client
            for node_port in nodes:
                if node_port == self.port or not node_status_manager.is_alive(node_port):
                    continue
                try:
                    cmd["internal"] = True
                    response = await forward_request(node_port, cmd)
                    if response.get("status") == STATUS_OK:
                        # Nếu một node khác có dữ liệu, trả về ngay
                        return response
                except Exception:
                    pass
            # Nếu không node nào có, trả về không tìm thấy
            return {"status": STATUS_NOT_FOUND, "message": f"Key '{key}' not found"}
# ...

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
                    except Exception:
                        pass
                return {"status": STATUS_OK, "message": f"Deleted {key}"}

            if not node_status_manager.is_alive(primary):
                return await self.act_as_temporary_primary(key, is_delete=True)

            try:
                return await forward_request(primary, cmd)
            except Exception:
                return await self.act_as_temporary_primary(key, is_delete=True)

        return {"status": STATUS_ERROR, "message": f"Unknown action: {action}"}
