import asyncio
import argparse
import json
import functools

print = functools.partial(print, flush=True)

from router_node import get_responsible_nodes, forward_request
from store_node import KVStore
from config import *

class KVNode:
    def __init__(self, host, port, log_callback=None):
        self.host = host
        self.port = port
        self.store_file = f"data/store_kv_node_{port - 8887}.json"
        self.kv = KVStore(self.store_file)
        self.log_callback = log_callback or print

    def log(self, message):
        self.log_callback(message)

    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info('peername')
        self.log(f"[{self.port}] Connected by {addr}")

        try:
            while True:
                try:
                    data = await reader.readline()
                    if not data:
                        break

                    message = json.loads(data.decode())
                    response = await self.process_command(message)

                except ConnectionResetError:
                    self.log(f"[{self.port}] Connection reset by {addr}")
                    break
                except Exception as e:
                    response = {"status": STATUS_ERROR, "message": f"Error: {str(e)}"}

                writer.write((json.dumps(response) + "\n").encode())
                await writer.drain()

        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionResetError:
                self.log(f"[{self.port}] Client closed connection: {addr}")
            self.log(f"[{self.port}] Disconnected: {addr}")

    async def act_as_temporary_primary(self, key, value=None, is_delete=False):
        if is_delete:
            deleted = key in self.kv.store
            if deleted:
                del self.kv.store[key]
                self.kv.save_store()
        else:
            existed = key in self.kv.store
            self.kv.store[key] = value
            self.kv.save_store()

        for replica_port in get_responsible_nodes(key):
            if replica_port == self.port:
                continue
            try:
                await forward_request(replica_port, {
                    "action": "replica_delete" if is_delete else "replica_put",
                    "key": key,
                    "value": value if not is_delete else None
                })
            except Exception as e:
                self.log(f"[{self.port}] [Fallback] Failed to contact replica {replica_port}: {e}")

        return {
            "status": STATUS_OK if (deleted if is_delete else True) else STATUS_NOT_FOUND,
            "message": f"[Fallback] {'Deleted' if is_delete else 'Stored'} {key}"
        }

    async def process_command(self, cmd):
        action = cmd.get("action", "").lower()
        key = cmd.get("key")
        value = cmd.get("value")
        if not action or not key:
            return {"status": STATUS_ERROR, "message": "Missing action or key"}

        nodes = get_responsible_nodes(key)

        if action == "replica_put":
            self.kv.store[key] = value
            self.kv.save_store()
            return {"status": STATUS_OK, "message": "Replicated"}

        if action == "replica_delete":
            if key in self.kv.store:
                del self.kv.store[key]
                self.kv.save_store()
                return {"status": STATUS_OK, "message": "Replica deleted"}
            return {"status": STATUS_NOT_FOUND, "message": "Key not found in replica"}

        if action == "put":
            primary = nodes[0]
            if self.port == primary:
                existed = key in self.kv.store
                self.kv.store[key] = value
                self.kv.save_store()

                for replica_port in nodes[1:]:
                    try:
                        await forward_request(replica_port, {
                            "action": "replica_put",
                            "key": key,
                            "value": value
                        })
                    except Exception as e:
                        self.log(f"[{self.port}] Replica PUT failed to {replica_port}: {e}")

                return {"status": STATUS_OK, "message": f"{'Updated' if existed else 'Stored'} {key}"}
            else:
                try:
                    self.log(f"[{self.port}] Forwarding PUT to primary {primary}")
                    return await forward_request(primary, cmd)
                except Exception as e:
                    self.log(f"[{self.port}] Primary {primary} unreachable for PUT: {e}")
                    return await self.act_as_temporary_primary(key, value)

        if action == "get":
            if key in self.kv.store:
                return {"status": STATUS_OK, "value": self.kv.store[key]}
            for node_port in nodes:
                if node_port == self.port:
                    continue
                try:
                    response = await forward_request(node_port, cmd)
                    if response.get("status") == STATUS_OK:
                        self.kv.store[key] = response["value"]
                        self.kv.save_store()
                        return response
                except Exception as e:
                    self.log(f"[{self.port}] GET forward to {node_port} failed: {e}")
            return {"status": STATUS_NOT_FOUND, "message": f"Key '{key}' not found"}

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

                return {"status": STATUS_OK if deleted else STATUS_NOT_FOUND,
                        "message": f"{'Deleted' if deleted else 'Key not found'} {key}"}
            else:
                try:
                    self.log(f"[{self.port}] Forwarding DELETE to primary {primary}")
                    return await forward_request(primary, cmd)
                except Exception as e:
                    self.log(f"[{self.port}] Primary {primary} unreachable for DELETE: {e}")
                    return await self.act_as_temporary_primary(key, is_delete=True)

        return {"status": STATUS_ERROR, "message": f"Unknown action: {action}"}

    async def start(self):
        self.server = await asyncio.start_server(self.handle_client, self.host, self.port)
        self.log(f"[Node {self.port}] started at {self.host}:{self.port}")
        try:
            async with self.server:
                await self.server.serve_forever()
        except asyncio.CancelledError:
            self.log(f"[Node {self.port}] stopped serving")

    async def stop(self):
        if hasattr(self, 'server'):
            self.server.close()
            await self.server.wait_closed()
            self.log(f"[Node {self.port}] stopped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    node = KVNode(
        host=NODE_HOST,
        port=args.port,
        log_callback=functools.partial(print, f"[Node {args.port}]", flush=True)
    )

    async def main():
        try:
            await node.start()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            node.log(f"Unexpected error: {e}")
        finally:
            node.log("Node shutting down...")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n[Node {args.port}] Ctrl+C received. Exiting...")
        import sys
        sys.exit(0)
