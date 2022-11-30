import socket
import threading
import select
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
    msg = "I reached zookeper function"
    conn.send(msg.encode(FORMAT))
    

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
    poller = select.poll()
    poller.register(server,select.POLLIN)
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 1}")

        evts = poller.poll(5000)
        for server, evt in evts:
            if evt and select.POLLIN:
                server.recvfrom(4096)
                print("Server sent poll event.")

if __name__ == "__main__":
    main()