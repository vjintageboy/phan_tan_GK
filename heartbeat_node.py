import asyncio
import json
import time
from config import NODE_PORTS as ALL_NODE_PORTS, HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, NODE_HOST
from colorama import Fore, Style


class HeartbeatManager:
    def __init__(self, port, log_callback=None):
        self.port = port
        self.last_heartbeat = {}  # Lưu thời gian nhận heartbeat cuối
        self.node_status = {}     # Lưu trạng thái hiện tại: ALIVE / DEAD
        self.ready = False        # Chờ tất cả node khởi động xong
        self.log_callback = log_callback or print
        self._running = True
        self.last_failed_log = {}  # Ghi nhớ lần cuối cùng đã log lỗi gửi heartbeat

    def log(self, message):
        self.log_callback(f" {message}")


    async def send_heartbeat(self):
        """Gửi heartbeat tới các node khác"""
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
                        # Chỉ log lỗi nếu node còn đang ALIVE
                        if now - last_log > 5 and self.node_status.get(target_port, "ALIVE") == "ALIVE":
                            self.log(f"Could not send heartbeat to {target_port}: {e}")
                            self.last_failed_log[target_port] = now
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def receive_heartbeat(self, reader, writer):
        """Xử lý heartbeat nhận được"""
        try:
            data = await reader.readline()
            if not data:
                return
            message = json.loads(data.decode())
            if message.get("type") == "heartbeat":
                sender_port = message.get("from")
                self.last_heartbeat[sender_port] = time.time()
                # Không cần log nhận heartbeat để giảm spam
        except Exception as e:
            self.log(f"Error processing heartbeat: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def start_server(self):
        """Lắng nghe các heartbeat đến"""
        server = await asyncio.start_server(self.receive_heartbeat, NODE_HOST, self.port + 1000)
        self.log(f"Listening for heartbeat at port {self.port + 1000}")
        async with server:
            await server.serve_forever()

    async def monitor_nodes(self):
        """Theo dõi trạng thái các node khác"""
        await asyncio.sleep(2)  # Chờ các node khác khởi động
        self.ready = True

        while self._running:
            now = time.time()
            for other_port in ALL_NODE_PORTS:
                if other_port == self.port:
                    continue
                last = self.last_heartbeat.get(other_port)
                alive = last and (now - last <= HEARTBEAT_TIMEOUT)
                old_status = self.node_status.get(other_port)
                new_status = "ALIVE" if alive else "DEAD"

                if old_status != new_status:
                    self.node_status[other_port] = new_status
                    last_seen = f"{now - last:.1f}s ago" if last else "never"
                    self.log(f"Node {other_port} is {new_status} (last seen {last_seen})")
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def start(self):
        """Khởi động heartbeat"""
        await asyncio.gather(
            self.send_heartbeat(),
            self.start_server(),
            self.monitor_nodes()
        )

    async def stop(self):
        """Dừng heartbeat"""
        self._running = False
