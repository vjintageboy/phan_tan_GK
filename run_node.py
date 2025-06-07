import tkinter as tk
from tkinter import ttk
import threading
import subprocess
import asyncio

from node import KVNode
from config import NODE_HOST, NODE_PORTS

node_threads = {}
status_labels = {}
node_buttons = {}

# === GUI Functions ===

def append_log(msg):
    log_text.config(state=tk.NORMAL)
    log_text.insert(tk.END, msg + "\n")
    log_text.see(tk.END)
    log_text.config(state=tk.DISABLED)

def update_node_status(node_id, running):
    if running:
        status_labels[node_id].config(text="Running", foreground="green")
        node_buttons[node_id].config(text="Crash", command=lambda n=node_id: crash_node(n))
    else:
        status_labels[node_id].config(text="Stopped", foreground="red")
        node_buttons[node_id].config(text="Restart", command=lambda n=node_id: restart_node(n))

def start_node(node_id):
    if node_id not in node_threads:
        loop = asyncio.new_event_loop()
        node = KVNode(NODE_HOST, NODE_PORTS[node_id])
        thread = threading.Thread(target=lambda: loop.run_until_complete(node.start()), daemon=True)
        thread.start()
        node_threads[node_id] = (node, thread, loop)
        append_log(f"[INFO] Node {node_id} started.")
        update_node_status(node_id, True)

def crash_node(node_id):
    if node_id in node_threads:
        node, thread, loop = node_threads[node_id]
        asyncio.run_coroutine_threadsafe(node.stop(), loop)
        del node_threads[node_id]
        append_log(f"[INFO] Node {node_id} crashed.")
        update_node_status(node_id, False)
    else:
        append_log(f"[WARN] Node {node_id} is not running.")


def restart_node(node_id):
    start_node(node_id)

def send_command():
    node_id = int(node_select.get())
    cmd_type = command_type.get().upper()
    key = key_entry.get().strip()
    value = value_entry.get().strip()

    if not key:
        append_log("[ERROR] Key is required.")
        return
    if cmd_type == "PUT" and not value:
        append_log("[ERROR] PUT command requires a value.")
        return

    args = ["python", "client.py",
            "--host", NODE_HOST,
            "--port", str(NODE_PORTS[node_id]),
            cmd_type, key]
    if cmd_type == "PUT":
        args.append(value)

    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=5)
        if result.stdout:
            for line in result.stdout.strip().splitlines():
                append_log(f"[Node {node_id}] {line}")
        if result.stderr:
            append_log(f"[Node {node_id}] STDERR: {result.stderr.strip()}")
    except subprocess.TimeoutExpired:
        append_log(f"[ERROR] Timeout while contacting node {node_id}")
    except Exception as e:
        append_log(f"[ERROR] Failed to run client.py: {e}")

# === GUI Setup ===

window = tk.Tk()
window.title("Distributed KV Store Control Panel")
window.geometry("800x600")

# --- Frame: Node Controls ---
node_frame = tk.LabelFrame(window, text="Node Controls", padx=10, pady=10)
node_frame.pack(padx=10, pady=10, fill="x")

for node_id in range(len(NODE_PORTS)):
    frame = tk.Frame(node_frame)
    frame.pack(fill="x", pady=2)

    tk.Label(frame, text=f"Node {node_id}").pack(side=tk.LEFT, padx=5)

    status = tk.Label(frame, text="Stopped", foreground="red")
    status.pack(side=tk.LEFT, padx=10)
    status_labels[node_id] = status

    btn = tk.Button(frame, text="Restart", command=lambda n=node_id: restart_node(n))
    btn.pack(side=tk.LEFT, padx=10)
    node_buttons[node_id] = btn

# --- Frame: Command Panel ---
cmd_frame = tk.LabelFrame(window, text="Send Command", padx=10, pady=10)
cmd_frame.pack(padx=10, pady=10, fill="x")

tk.Label(cmd_frame, text="Node:").grid(row=0, column=0, sticky="e")
node_select = ttk.Combobox(cmd_frame, values=[str(i) for i in range(len(NODE_PORTS))])
node_select.grid(row=0, column=1)
node_select.set("0")

tk.Label(cmd_frame, text="Command:").grid(row=0, column=2, sticky="e")
command_type = ttk.Combobox(cmd_frame, values=["PUT", "GET", "DELETE"])
command_type.grid(row=0, column=3)
command_type.set("PUT")

tk.Label(cmd_frame, text="Key:").grid(row=1, column=0, sticky="e")
key_entry = tk.Entry(cmd_frame)
key_entry.grid(row=1, column=1, padx=5)

tk.Label(cmd_frame, text="Value:").grid(row=1, column=2, sticky="e")
value_entry = tk.Entry(cmd_frame)
value_entry.grid(row=1, column=3, padx=5)

tk.Button(cmd_frame, text="Send", command=send_command).grid(row=2, column=0, columnspan=4, pady=5)

# --- Frame: Log ---
log_frame = tk.LabelFrame(window, text="Log Output", padx=10, pady=10)
log_frame.pack(padx=10, pady=10, fill="both", expand=True)

log_text = tk.Text(log_frame, height=20, state=tk.DISABLED, wrap=tk.WORD)
log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

scrollbar = tk.Scrollbar(log_frame, command=log_text.yview)
scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
log_text.config(yscrollcommand=scrollbar.set)

# === Run App ===
window.mainloop()
