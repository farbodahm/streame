# Sample script to spin up a TCP server and write into it interactively
import socket
import threading


def handle_client(conn: socket.socket, addr: str):
    print(f"Connected by {addr}")
    try:
        while True:
            data = input("Enter message to send (type 'exit' to quit): ")
            if data.lower() == "exit":
                break
            conn.sendall(data.encode() + b"\n")
    except ConnectionResetError:
        print(f"Connection with {addr} has been closed.")
    finally:
        conn.close()


def start_server(host: str, port: int):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen()
        print(f"Server listening on {host}:{port}")

        while True:
            conn, addr = s.accept()
            client_thread = threading.Thread(target=handle_client, args=(conn, addr))
            client_thread.start()
            # Wait for the thread to finish to ensure only one client is handled at a time
            client_thread.join()
            break  # Stop the server after handling one connection for simplicity


if __name__ == "__main__":
    start_server(host="localhost", port=9999)
