import socket
import json

IP = socket.gethostbyname(socket.gethostname())

Z_PORT = 9090
Z_ADDR = (IP, Z_PORT)

PORT = 5566
ADDR = (IP, PORT)

SIZE = 1024
FORMAT = "utf-8"
DISCONNECT_MSG = "!DISCONNECT"

def main():

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(Z_ADDR)
    print(f"[CONNECTED] Client connected to zookeeper at {IP}:{Z_PORT}")

    connected = True
    while connected:
        
        message = "1"
        client.send(message.encode())

        if msg == DISCONNECT_MSG:
            connected = False
        else:
            msg = client.recv(SIZE).decode(FORMAT)
            print(f"[SERVER] {msg}")

        client.send(data.encode())

        if msg == DISCONNECT_MSG:
            connected = False
        else:
            msg = client.recv(SIZE).decode(FORMAT)
            print(f"[SERVER] {msg}")   
    #---------------------------------------------------------------------------------------------------
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(ADDR)
    print(f"[CONNECTED] Client connected to zookeeper at {IP}:{PORT}")

    connected = True
    while connected:
        print("Enter topic name: ")
        topic = input()
        print("Enter the desired message:")
        msg = input()

        data = {topic:msg}

        data = json.dumps(data) #serialising
    
        message = "1"
        client.send(message.encode())

        if msg == DISCONNECT_MSG:
            connected = False
        else:
            msg = client.recv(SIZE).decode(FORMAT)
            print(f"[SERVER] {msg}")

        client.send(data.encode())

        if msg == DISCONNECT_MSG:
            connected = False
        else:
            msg = client.recv(SIZE).decode(FORMAT)
            print(f"[SERVER] {msg}")

if __name__ == "__main__":
    main()