import hashlib
import json
import asyncio

from config import NODE_PORTS as ALL_NODES


def hash_key(key):
    return int(hashlib.sha256(key.encode()).hexdigest(), 16)

def get_responsible_node(key):
    h = hash_key(key)
    return ALL_NODES[h % len(ALL_NODES)]

def get_responsible_nodes(key, replica_count=2):
    h = hash_key(key)
    idx = h % len(ALL_NODES)
    return [ALL_NODES[(idx + i) % len(ALL_NODES)] for i in range(replica_count)]

async def forward_request(target_port, data):
    try:
        reader, writer = await asyncio.open_connection('127.0.0.1', target_port)
        message = json.dumps(data) + '\n'
        writer.write(message.encode())
        await writer.drain()

        response_data = await reader.readline()
        if not response_data:
            raise ConnectionError("Empty response received")

        response = json.loads(response_data.decode())

        writer.close()
        await writer.wait_closed()

        return response
    except Exception as e:
        print(f"[{target_port}] Forwarding error: {e}")  # Gợi ý thêm log
        return {"status": "ERROR", "message": f"Forwarding failed: {str(e)}"}
