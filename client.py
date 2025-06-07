import asyncio
import json
import argparse

from router_node import get_responsible_nodes
from config import NODE_PORTS  

async def send_command_to_node(host, port, command):
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except ConnectionRefusedError:
        print(f"Connection refused at {host}:{port}")
        return None
    except Exception as e:
        print(f"Could not connect to {host}:{port} - {e}")
        return None

    try:
        writer.write((json.dumps(command) + "\n").encode())
        await writer.drain()

        data = await reader.readline()
        if not data:
            print("No data received or connection closed prematurely.")
            return None

        try:
            response = json.loads(data.decode().strip())
        except json.JSONDecodeError:
            print(f"Could not decode response: {data.decode()}")
            return None

        return response

    except asyncio.IncompleteReadError:
        print("Connection closed prematurely by server.")
    except Exception as e:
        print(f"Communication error: {e}")
    finally:
        if writer and not writer.is_closing():
            writer.close()
            await writer.wait_closed()
    return None

async def send_command(command):
    nodes = get_responsible_nodes(command["key"])

    for port in nodes:
        print(f"Trying node at port {port}...")
        response = await send_command_to_node("127.0.0.1", port, command)
        if response:
            status = response.get("status")
            if status == "OK":
                if command.get("action") == "GET":
                    print(f"Value: {response.get('value')}")
                else:
                    print(f"Success: {response.get('message', 'Operation successful.')}")
            elif status == "NOT_FOUND":
                print(f"Not found at node {port}: Key '{command.get('key')}'")
            elif status == "ERROR":
                print(f"Server error at node {port}: {response.get('message')}")
            else:
                print(f"Response from node {port}:", response)
            return  
        else:
            print(f" Failed to get response from node {port}. Trying next...")

    print("All responsible nodes failed or unreachable.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Client for a distributed Key-Value store.")
    parser.add_argument("action", choices=["PUT", "GET", "DELETE"], type=str.upper, help="Action to perform.")
    parser.add_argument("key", help="Key for the operation.")
    parser.add_argument("value", nargs="?", help="Value (only required for PUT).")
    args = parser.parse_args()

    command = {"action": args.action, "key": args.key}
    if args.action == "PUT":
        if args.value is None:
            parser.error("PUT action requires a value argument.")
        command["value"] = args.value
    elif args.value is not None:
        print(f"⚠️ Value argument '{args.value}' is ignored for {args.action} action.")

    asyncio.run(send_command(command))
