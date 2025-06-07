import asyncio
import argparse
import json

from store_node import KVStore
from action_node import KVNodeLogic
from heartbeat_node import HeartbeatManager
from config import *

def make_logger(name):
    def log(msg):
        print(f"{name} {msg}", flush=True)
    return log

class KVNode:
    def __init__(self, host, port, log_callback=None):
        self.host = host
        self.port = port
        self.store_file = f"data/store_kv_node_{port - 8887}.json"
        self.kv = KVStore(self.store_file)
        self.log_callback = log_callback or make_logger(f"Node {self.port}")
        self.logic = KVNodeLogic(self.kv, self.port, self.log_callback)

    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info('peername')
        self.log_callback(f"Connected by {addr}")

        try:
            while True:
                try:
                    data = await reader.readline()
                    if not data:
                        break

                    message = json.loads(data.decode())
                    response = await self.logic.handle(message)

                except ConnectionResetError:
                    self.log_callback(f"Connection reset by {addr}")
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
                self.log_callback(f"Client closed connection: {addr}")
            self.log_callback(f"Disconnected: {addr}")

    async def start(self):
        self.server = await asyncio.start_server(self.handle_client, self.host, self.port)
        self.log_callback(f"started at {self.host}:{self.port}")
        try:
            async with self.server:
                await self.server.serve_forever()
        except asyncio.CancelledError:
            self.log_callback("stopped serving")
        await self.sync_missing_data()

    async def sync_missing_data(self):
        pass

    async def stop(self):
        if hasattr(self, 'server'):
            self.server.close()
            await self.server.wait_closed()
            self.log_callback("stopped")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    node_logger = make_logger(f"")
    heartbeat_logger = make_logger(f"")

    node = KVNode(
        host=NODE_HOST,
        port=args.port,
        log_callback=node_logger
    )

    heartbeat = HeartbeatManager(
    port=args.port,
    log_callback=heartbeat_logger
)


    async def main():
        await asyncio.gather(
            node.start(),
            heartbeat.start()
        )

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n[Node {args.port}] Ctrl+C received. Exiting...", flush=True)
        import sys
        sys.exit(0)
