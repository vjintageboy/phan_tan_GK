import asyncio
import argparse
import json
import logging

from store_node import KVStore
from action_node import KVNodeLogic
from heartbeat_node import HeartbeatManager
from config import *

# Thiết lập logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        self.log_callback = log_callback or make_logger(f"[Node {self.port}]")
        self.logic = KVNodeLogic(self.kv, self.port, self.log_callback)

    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info('peername')
        logger.debug(f"[Node {self.port}] Connected by {addr}")

        try:
            while True:
                data = await reader.readline()
                if not data:
                    break

                try:
                    message = json.loads(data.decode())
                    response = await self.logic.handle(message)
                except Exception as e:
                    logger.debug(f"[Node {self.port}] Error handling request from {addr}: {e}")
                    response = {"status": STATUS_ERROR, "message": f"Error: {str(e)}"}

                writer.write((json.dumps(response) + "\n").encode())
                await writer.drain()

        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            logger.debug(f"[Node {self.port}] Disconnected: {addr}")

    async def start(self):
        self.server = await asyncio.start_server(self.handle_client, self.host, self.port)
        self.log_callback(f"started at {self.host}:{self.port}")

        # Bắt đầu sync dữ liệu sau khi server khởi động
        asyncio.create_task(self.sync_missing_data())

        try:
            async with self.server:
                await self.server.serve_forever()
        except asyncio.CancelledError:
            self.log_callback("stopped serving")

    async def sync_missing_data(self):
        await asyncio.sleep(3)  # Đợi các node khác phát hiện node này sống lại
        self.log_callback("Syncing missing data after recovery...")
        await self.logic.sync_missing_data()

    async def stop(self):
        if hasattr(self, 'server'):
            self.server.close()
            await self.server.wait_closed()
            self.log_callback("stopped")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    node_logger = make_logger(f"[Node {args.port}]")
    heartbeat_logger = make_logger(f"[Heartbeat {args.port}]")

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