import asyncio
import json
import argparse

from config import NODE_PORTS  

async def send_command(host, port, command):
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except ConnectionRefusedError:
        print(f"Error: Connection refused by server at {host}:{port}")
        return
    except Exception as e:
        print(f"Error: Could not connect to {host}:{port}. {e}")
        return

    try:
        writer.write((json.dumps(command) + "\n").encode())
        await writer.drain()

        data = await reader.readline()
        if not data:
            print("Error: No data received from server or connection closed prematurely.")
            return

        try:
            response = json.loads(data.decode().strip())
        except json.JSONDecodeError:
            print(f"Error: Could not decode JSON response from server: {data.decode()}")
            return

        status = response.get("status")
        if status == "OK":
            if command.get("action") == "GET":
                print(f"Value: {response.get('value')}")
            else:
                print(f"Success: {response.get('message', 'Operation successful.')}")
        elif status == "NOT_FOUND":
            print(f"Error: Key '{command.get('key')}' not found by server at {host}:{port}.")
        elif status == "ERROR":
            print(f"Server Error: {response.get('message', 'An unspecified error occurred on the server.')}")
        else:
            print("Response:", response)

    except asyncio.IncompleteReadError:
        print("Error: Connection closed prematurely by the server while awaiting response.")
    except Exception as e:
        print(f"An error occurred during communication: {e}")
    finally:
        if 'writer' in locals() and writer and not writer.is_closing():
            writer.close()
            await writer.wait_closed()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Client for a distributed Key-Value store.")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Server host address.")
    parser.add_argument("--port", type=int, default= NODE_PORTS[0], help="Server port number.")
    parser.add_argument("action", choices=["PUT", "GET", "DELETE"], type=str.upper, help="Action to perform (PUT, GET, DELETE).")
    parser.add_argument("key", help="The key for the action.")
    parser.add_argument("value", nargs='?', help="The value for PUT action.")
    args = parser.parse_args()

    command = {"action": args.action, "key": args.key}
    if args.action == "PUT":
        if args.value is None:
            parser.error("PUT action requires a value argument.")
        command["value"] = args.value
    elif args.value is not None: # For GET or DELETE
        print(f"Warning: Value argument '{args.value}' is ignored for {args.action} action.")

    asyncio.run(send_command(args.host, args.port, command))
