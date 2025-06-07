import subprocess
import threading
import time
import sys
import os
import signal

from config import NODE_PORTS as ports

COLORS = {
    8888: '\033[96m',   # Cyan
    8889: '\033[93m',   # Yellow
    8890: '\033[92m',   # Green
}
RESET = '\033[0m'

processes = {}  # port -> process

def stream_output(pipe, prefix, color):
    try:
        for line in iter(pipe.readline, ''):
            if not line:
                break
            print(f"{color}[{prefix}] {line.strip()}{RESET}")
    except Exception as e:
        print(f"{color}[{prefix} ERROR] {e}{RESET}")
    finally:
        pipe.close()

def start_node(port):
    print(f"Starting node at port {port}...")
    try:
        process = subprocess.Popen(
            [sys.executable, "node.py", "--port", str(port)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        processes[port] = process

        threading.Thread(
            target=stream_output,
            args=(process.stdout, f"Node {port}", COLORS.get(port, '')),
            daemon=True
        ).start()
    except Exception as e:
        print(f"Failed to start node at port {port}: {e}")

def crash_node(port):
    if port in processes and processes[port].poll() is None:
        print(f"Crashing node {port}")
        try:
            os.kill(processes[port].pid, signal.SIGKILL)
        except Exception as e:
            print(f"Error crashing node {port}: {e}")
    else:
        print(f"Node {port} is not running")

def restart_node(port):
    print(f"Restarting node {port}...")
    crash_node(port)
    time.sleep(1)
    start_node(port)

# --- Khởi động tất cả ban đầu ---
for port in ports:
    start_node(port)
    time.sleep(0.5)

# --- CLI nhập lệnh ---
try:
    while True:
        cmd = input("> ").strip()
        if not cmd:
            continue
        if cmd == "exit":
            break

        parts = cmd.split()
        if len(parts) == 2:
            action, port_str = parts
            try:
                port = int(port_str)
                if action == "crash":
                    crash_node(port)
                elif action == "restart":
                    restart_node(port)
                else:
                    print("Unknown command. Use: crash <port> | restart <port>")
            except ValueError:
                print("Port phải là số")
        else:
            print("Cú pháp: crash <port> | restart <port> | exit")
except KeyboardInterrupt:
    print("\nDừng tất cả node...")

# --- Dọn dẹp ---
for p in processes.values():
    if p.poll() is None:
        try:
            os.kill(p.pid, signal.SIGTERM)
        except Exception as e:
            print(f"Error stopping process {p.pid}: {e}")
