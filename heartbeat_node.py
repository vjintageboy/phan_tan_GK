import asyncio
import json
import time
from config import NODE_PORTS as ALL_NODE_PORTS, HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, NODE_HOST
from node_status_manager import node_status_manager  # dùng singleton


class HeartbeatManager:
    def __init__(self, port, log_callback=None):
        self.port = port
        self.ready = False
        self.log_callback = log_callback or print
        self._running = True
        self.last_failed_log = {}  # để tránh log trùng lặp

    def log(self, message):
        self.log_callback(f" {message}")

    async def send_heartbeat(self):
        while self._running:
            for target_port in ALL_NODE_PORTS:
                if target_port == self.port:
                    continue
                try:
                    reader, writer = await asyncio.open_connection(NODE_HOST, target_port + 1000)
                    message = {"type": "heartbeat", "from": self.port}
                    writer.write((json.dumps(message) + "\n").encode())
                    await writer.drain()
                    writer.close()
                    await writer.wait_closed()
                except Exception as e:
                    if self.ready:
                        now = time.time()
                        last_log = self.last_failed_log.get(target_port, 0)
                        if now - last_log > 5 and node_status_manager.is_alive(target_port, HEARTBEAT_TIMEOUT):
                            self.log(f"Could not send heartbeat to {target_port}: {e}")
                            self.last_failed_log[target_port] = now
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def receive_heartbeat(self, reader, writer):
        try:
            data = await reader.readline()
            if not data:
                return
            message = json.loads(data.decode())
            if message.get("type") == "heartbeat":
                sender_port = message.get("from")
                node_status_manager.update(sender_port)  # ✅ Cập nhật tại đây
        except Exception as e:
            self.log(f"Error processing heartbeat: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def start_server(self):
        server = await asyncio.start_server(self.receive_heartbeat, NODE_HOST, self.port + 1000)
        self.log(f"Listening for heartbeat at port {self.port + 1000}")
        async with server:
            await server.serve_forever()

    async def monitor_nodes(self):
        await asyncio.sleep(2)
        self.ready = True

        prev_status = {}  # Lưu trạng thái trước đó

        while self._running:
            status_dict = node_status_manager.get_all_statuses(HEARTBEAT_TIMEOUT)
            for node_id, status in status_dict.items():
                if node_id == self.port:
                    continue
                old_status = prev_status.get(node_id)
                if old_status != status:
                    self.log(f"Node {node_id} is {status}")
                    prev_status[node_id] = status  # Cập nhật trạng thái mới
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def start(self):
        await asyncio.gather(
            self.send_heartbeat(),
            self.start_server(),
            self.monitor_nodes()
        )

    async def stop(self):
        self._running = False