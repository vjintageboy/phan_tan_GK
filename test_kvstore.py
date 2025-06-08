import asyncio
import pytest
import subprocess
import time

from router_node import forward_request
from config import STATUS_OK, STATUS_NOT_FOUND

ALL_PORTS = [8888, 8889, 8890]
KEY = "testkey"
VALUE1 = "Hello"
VALUE2 = "World"

async def wait_node_ready(port, retries=10, delay=1):
    for _ in range(retries):
        try:
            resp = await forward_request(port, {"action": "ping"})
            if resp.get("status") == "OK":
                print(f"[Check] Node {port} is ready")
                return
        except Exception:
            print(f"[Check] Waiting for node {port}...")
        await asyncio.sleep(delay)
    raise RuntimeError(f"Node {port} not responding after {retries} retries.")

@pytest.mark.asyncio
async def test_put_get_delete():
    """Kiểm thử PUT, GET, DELETE cơ bản với nhiều node."""

    # Đảm bảo các node đã sẵn sàng
    for port in ALL_PORTS:
        await wait_node_ready(port)

    # PUT lần đầu
    resp = await forward_request(8888, {
        "action": "put",
        "key": KEY,
        "value": VALUE1
    })
    assert resp["status"] == STATUS_OK

    # GET ở node khác
    resp = await forward_request(8889, {
        "action": "get",
        "key": KEY
    })
    assert resp["status"] == STATUS_OK
    assert resp["value"]["value"] == VALUE1

    # PUT cập nhật giá trị
    resp = await forward_request(8890, {
        "action": "put",
        "key": KEY,
        "value": VALUE2
    })
    assert resp["status"] == STATUS_OK

    # GET sau cập nhật
    resp = await forward_request(8889, {
        "action": "get",
        "key": KEY
    })
    assert resp["status"] == STATUS_OK
    assert resp["value"]["value"] == VALUE2

    # DELETE
    resp = await forward_request(8890, {
        "action": "delete",
        "key": KEY
    })
    assert resp["status"] == STATUS_OK

    # GET sau DELETE → phải NOT_FOUND
    resp = await forward_request(8888, {
        "action": "get",
        "key": KEY
    })
    assert resp["status"] == STATUS_NOT_FOUND

@pytest.mark.asyncio
async def test_restart_and_sync():
    """Giả lập node chết, sau đó restart và kiểm tra đồng bộ tombstone."""

    key = "sync_test"
    value = "Data"

    for port in [8888, 8889]:
        await wait_node_ready(port)

    # Gửi PUT đến node sống
    resp = await forward_request(8888, {
        "action": "put",
        "key": key,
        "value": value
    })
    assert resp["status"] == STATUS_OK

    # Kill node 8890 (giả lập node chết)
    subprocess.call(["pkill", "-f", "run_node.py.*8890"])
    print("[Test] Node 8890 crashed")
    await asyncio.sleep(2)

    # DELETE trong lúc 8890 chết
    resp = await forward_request(8889, {
        "action": "delete",
        "key": key
    })
    assert resp["status"] == STATUS_OK

    # Restart lại node 8890
    subprocess.Popen(["python", "run_node.py", "--port", "8890"])
    print("[Test] Node 8890 restarting")
    await asyncio.sleep(5)  # chờ node khởi động & sync

    await wait_node_ready(8890)

    # GET lại key tại node 8890, phải NOT_FOUND (đã bị xoá và sync)
    resp = await forward_request(8890, {
        "action": "get",
        "key": key
    })
    assert resp["status"] == STATUS_NOT_FOUND
