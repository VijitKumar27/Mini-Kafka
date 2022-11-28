import socket
import threading
#import wikipedia
import json
import os

IP = socket.gethostbyname(socket.gethostname())
PORT = 9090
ADDR = (IP, PORT)
SIZE = 1024
FORMAT = "utf-8"
DISCONNECT_MSG = "!DISCONNECT"

def handle_producer(conn, addr):
    print('x')
    

def handle_consumer(conn, addr):
    print('y')
    
            

def handle_client(conn, addr):
    print(f"[NEW CONNECTION] {addr} connected.")

    connected = True
    while connected:
        check = conn.recv(SIZE).decode(FORMAT)
        
        if(check == "1"):
            handle_producer(conn, addr)

        if(check == "2"):
            handle_consumer(conn, addr)
    conn.close()
        

def main():
    print("[STARTING] Zookeeper is starting...")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)
    server.listen()
    print(f"[LISTENING] Zookeeper is listening on {IP}:{PORT}")

    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 1}")

if __name__ == "__main__":
    main()