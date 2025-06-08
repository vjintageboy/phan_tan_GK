import tkinter as tk
from tkinter import scrolledtext, messagebox, ttk
import asyncio
import threading
import json
import subprocess
import sys
import os
import signal
import time

# Import các thành phần cần thiết từ dự án của bạn
from config import NODE_PORTS
from router_node import get_responsible_nodes

# Màu sắc để phân biệt output của các node
COLORS = {
    8888: 'darkcyan',
    8889: 'goldenrod',
    8890: 'darkgreen',
    'CLIENT': 'darkblue',
    'SYSTEM': 'red'
}
UPDATE_INTERVAL_MS = 2000 # Cập nhật hiển thị mỗi 2 giây

class KeyValueGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Distributed KV Store Controller")
        self.root.geometry("1100x750")

        self.processes = {}

        self.async_loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._start_asyncio_loop, daemon=True)
        self.thread.start()

        self._setup_widgets()
        
        self.root.after(500, self.start_all_nodes)
        
        # Bắt đầu vòng lặp cập nhật hiển thị real-time
        self.root.after(UPDATE_INTERVAL_MS, self.schedule_periodic_update)

        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

    def _start_asyncio_loop(self):
        asyncio.set_event_loop(self.async_loop)
        self.async_loop.run_forever()

    def _setup_widgets(self):
        # --- BỐ CỤC CHÍNH ---
        main_paned_window = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        main_paned_window.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # --- CỘT TRÁI (CONTROLS & LOG) ---
        left_frame = ttk.Frame(main_paned_window, width=500)
        main_paned_window.add(left_frame, weight=2)

        # --- CỘT PHẢI (REAL-TIME DISPLAYS) ---
        right_frame = ttk.Frame(main_paned_window, width=600)
        main_paned_window.add(right_frame, weight=3)

        self._setup_left_pane(left_frame)
        self._setup_right_pane(right_frame)

    def _setup_left_pane(self, parent):
        # Khung quản lý node
        node_frame = ttk.LabelFrame(parent, text="Node Management", padding=(10, 5))
        node_frame.pack(fill=tk.X, pady=(0, 10), padx=5)
        self.selected_node = tk.StringVar(value=str(NODE_PORTS[0]))
        node_menu = ttk.OptionMenu(node_frame, self.selected_node, str(NODE_PORTS[0]), *[str(p) for p in NODE_PORTS])
        node_menu.pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(node_frame, text="Crash Node", command=self.crash_selected_node).pack(side=tk.LEFT, padx=5)
        ttk.Button(node_frame, text="Restart Node", command=self.restart_selected_node).pack(side=tk.LEFT, padx=5)

        # --- THÊM NÚT QUIT TẠI ĐÂY ---
        # Thêm một đường kẻ dọc để ngăn cách
        ttk.Separator(node_frame, orient=tk.VERTICAL).pack(side=tk.LEFT, padx=(10, 5), fill='y')
        # Thêm nút Quit
        ttk.Button(node_frame, text="Quit Application", command=self.on_closing).pack(side=tk.LEFT, padx=5)
        # --- KẾT THÚC PHẦN THÊM MỚI ---
        
        # Khung thao tác Key-Value
        kv_frame = ttk.LabelFrame(parent, text="Key-Value Operations", padding=(10, 5))
        kv_frame.pack(fill=tk.X, pady=(0, 10), padx=5)
        ttk.Label(kv_frame, text="Key:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.key_entry = ttk.Entry(kv_frame, width=40)
        self.key_entry.grid(row=0, column=1, sticky=tk.EW, padx=5)
        ttk.Label(kv_frame, text="Value:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        self.value_entry = ttk.Entry(kv_frame, width=40)
        self.value_entry.grid(row=1, column=1, sticky=tk.EW, padx=5)
        kv_frame.columnconfigure(1, weight=1)
        button_frame = tk.Frame(kv_frame)
        button_frame.grid(row=2, column=0, columnspan=2, pady=10)
        ttk.Button(button_frame, text="GET", command=self.handle_get).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="PUT", command=self.handle_put).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="DELETE", command=self.handle_delete).pack(side=tk.LEFT, padx=5)

        # Khung Log Output
        log_frame = ttk.LabelFrame(parent, text="Log Output", padding=(10, 5))
        log_frame.pack(fill=tk.BOTH, expand=True, padx=5)
        self.log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, state=tk.DISABLED, height=10)
        self.log_text.pack(fill=tk.BOTH, expand=True)
        for port, color in COLORS.items():
            self.log_text.tag_config(str(port), foreground=color)

    def _setup_right_pane(self, parent):
        # Khung trạng thái các node
        status_frame = ttk.LabelFrame(parent, text="Node Status", padding=(10, 5))
        status_frame.pack(fill=tk.X, pady=(0, 10), padx=5)
        self.status_display_text = scrolledtext.ScrolledText(status_frame, wrap=tk.WORD, state=tk.DISABLED, height=5)
        self.status_display_text.pack(fill=tk.X, expand=True)

        # Khung hiển thị dữ liệu từng node
        stores_frame = ttk.LabelFrame(parent, text="Node Key-Value Stores", padding=(10, 5))
        stores_frame.pack(fill=tk.BOTH, expand=True, padx=5)
        stores_frame.rowconfigure(0, weight=1)
        
        self.node_displays = {}
        for i, port in enumerate(NODE_PORTS):
            stores_frame.columnconfigure(i, weight=1)
            node_store_frame = ttk.LabelFrame(stores_frame, text=f"Node {port} Store")
            node_store_frame.grid(row=0, column=i, sticky="nsew", padx=5, pady=5)
            text_widget = scrolledtext.ScrolledText(node_store_frame, wrap=tk.WORD, state=tk.DISABLED, height=10)
            text_widget.pack(fill=tk.BOTH, expand=True)
            self.node_displays[port] = text_widget

    # --- Chức năng cập nhật Real-time ---

    def schedule_periodic_update(self):
        """Lên lịch chạy tác vụ cập nhật trong thread asyncio và lặp lại."""
        asyncio.run_coroutine_threadsafe(self._update_displays_async(), self.async_loop)
        self.root.after(UPDATE_INTERVAL_MS, self.schedule_periodic_update)

    async def _update_displays_async(self):
        """Tác vụ bất đồng bộ để lấy và cập nhật tất cả các hiển thị."""
        # Chạy song song việc lấy status và data của các node
        await asyncio.gather(
            self._fetch_and_update_status(),
            *[self._fetch_and_update_node_data(port) for port in NODE_PORTS]
        )

    async def _fetch_and_update_status(self):
        """Lấy trạng thái từ một node bất kỳ và cập nhật GUI."""
        cmd = {"action": "get_status"}
        for port in NODE_PORTS: # Thử từng node cho đến khi có kết quả
            if self.processes.get(port) and self.processes[port].poll() is None:
                try:
                    response = await self._send_internal_command_async(port, cmd)
                    if response and response.get("status") == "OK":
                        status_text = json.dumps(response.get("data", {}), indent=2)
                        self.update_text_widget(self.status_display_text, status_text)
                        return
                except Exception:
                    continue # Thử node tiếp theo
        # Nếu không node nào trả lời
        self.update_text_widget(self.status_display_text, "Could not fetch status from any node.")

    async def _fetch_and_update_node_data(self, port):
        """Lấy toàn bộ dữ liệu từ một node cụ thể và cập nhật GUI."""
        widget = self.node_displays[port]
        # Nếu node đã bị crash, hiển thị trạng thái DEAD
        if not (self.processes.get(port) and self.processes[port].poll() is None):
            self.update_text_widget(widget, "-- NODE IS DEAD --")
            return

        cmd = {"action": "get_all_data"}
        try:
            response = await self._send_internal_command_async(port, cmd)
            if response and response.get("status") == "OK":
                data_text = json.dumps(response.get("data", {}), indent=2)
                self.update_text_widget(widget, data_text)
            else:
                self.update_text_widget(widget, f"-- FAILED TO FETCH DATA --\n{response}")
        except Exception as e:
            self.update_text_widget(widget, f"-- NODE UNREACHABLE --\n{type(e).__name__}")
    
    async def _send_internal_command_async(self, port, command):
        """Hàm helper để gửi các lệnh nội bộ lấy thông tin."""
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", port), timeout=1.0
            )
            writer.write((json.dumps(command) + "\n").encode())
            await writer.drain()
            data = await asyncio.wait_for(reader.readline(), timeout=1.0)
            writer.close()
            await writer.wait_closed()
            return json.loads(data.decode().strip()) if data else None
        except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
            # Bắt các lỗi kết nối thường gặp
            raise

    def update_text_widget(self, widget, content):
        """Cập nhật nội dung của một widget Text một cách an toàn từ thread."""
        def _task():
            widget.config(state=tk.NORMAL)
            widget.delete('1.0', tk.END)
            widget.insert('1.0', content)
            widget.config(state=tk.DISABLED)
        self.root.after(0, _task)

    # --- Các chức năng cũ (giữ nguyên, chỉ chỉnh sửa nhỏ) ---
    # ... (Các hàm log, start_node, crash_node, restart_node, handlers, v.v. giữ nguyên từ phiên bản trước)
    def log(self, message, source='SYSTEM'):
        def _log():
            self.log_text.config(state=tk.NORMAL)
            self.log_text.insert(tk.END, f"[{source}] {message}\n", str(source))
            self.log_text.config(state=tk.DISABLED)
            self.log_text.see(tk.END)
        self.root.after(0, _log)

    def _stream_output(self, pipe, color_tag):
        try:
            for line in iter(pipe.readline, ''):
                if line:
                    self.log(line.strip(), color_tag)
        finally:
            pipe.close()

    def start_node(self, port):
        self.log(f"Starting node at port {port}...", 'SYSTEM')
        try:
            process = subprocess.Popen(
                [sys.executable, "node.py", "--port", str(port)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
            )
            self.processes[port] = process
            threading.Thread(target=self._stream_output, args=(process.stdout, port), daemon=True).start()
        except Exception as e:
            self.log(f"Failed to start node {port}: {e}", 'SYSTEM')

    def start_all_nodes(self):
        for port in NODE_PORTS:
            self.start_node(port)
            time.sleep(0.5)

    def crash_node(self, port):
        if port in self.processes and self.processes[port].poll() is None:
            self.log(f"Crashing node {port}...", 'SYSTEM')
            try:
                os.kill(self.processes[port].pid, signal.SIGKILL)
                self.log(f"Node {port} crashed.", 'SYSTEM')
            except Exception as e:
                self.log(f"Error crashing node {port}: {e}", 'SYSTEM')
        else:
            self.log(f"Node {port} is not running.", 'SYSTEM')

    def restart_node(self, port):
        self.log(f"Restarting node {port}...", 'SYSTEM')
        self.crash_node(port)
        self.root.after(1000, lambda: self.start_node(port))

    def crash_selected_node(self):
        self.crash_node(int(self.selected_node.get()))
        
    def restart_selected_node(self):
        self.restart_node(int(self.selected_node.get()))

    def handle_put(self):
        key = self.key_entry.get()
        value = self.value_entry.get()
        if not key or not value:
            messagebox.showerror("Error", "PUT action requires both a key and a value.")
            return
        self.submit_command({"action": "PUT", "key": key, "value": value})

    def handle_get(self):
        key = self.key_entry.get()
        if not key:
            messagebox.showerror("Error", "GET action requires a key.")
            return
        self.submit_command({"action": "GET", "key": key})

    def handle_delete(self):
        key = self.key_entry.get()
        if not key:
            messagebox.showerror("Error", "DELETE action requires a key.")
            return
        self.submit_command({"action": "DELETE", "key": key})

    def submit_command(self, command):
        asyncio.run_coroutine_threadsafe(self._send_command_async(command), self.async_loop)

    async def _send_command_async(self, command):
        key = command.get("key")
        self.log(f"Executing: {command['action']} key='{key}'", 'CLIENT')
        nodes = get_responsible_nodes(key)
        self.log(f"Responsible nodes for key '{key}': {nodes}", 'CLIENT')

        for port in nodes:
            self.log(f"Trying node at port {port}...", 'CLIENT')
            try:
                response = await self._send_internal_command_async(port, command)
                if response:
                    status = response.get("status")
                    if status == "OK":
                        if command.get("action") == "GET":
                            self.log(f" Success from node {port}. Value: {response.get('value')}", 'CLIENT')
                        else:
                            self.log(f" Success from node {port}: {response.get('message', 'Op successful.')}", 'CLIENT')
                    else:
                        self.log(f" Node {port} response: {response.get('message')}", 'CLIENT')
                    return
                else:
                    self.log(f" Failed to get response from node {port}. Trying next...", 'CLIENT')
            except Exception:
                self.log(f"Connection to node {port} failed. Trying next...", 'CLIENT')
        
        self.log(f"All responsible nodes for key '{key}' failed or are unreachable.", 'CLIENT')

    def on_closing(self):
        if messagebox.askokcancel("Quit", "Do you want to stop all nodes and quit?"):
            self.log("Shutting down all nodes...", 'SYSTEM')
            for port in self.processes:
                if self.processes[port].poll() is None:
                    try: os.kill(self.processes[port].pid, signal.SIGTERM)
                    except Exception: pass
            
            self.async_loop.call_soon_threadsafe(self.async_loop.stop)
            self.thread.join(timeout=2)
            self.root.destroy()

if __name__ == "__main__":
    root = tk.Tk()
    app = KeyValueGUI(root)
    root.mainloop()