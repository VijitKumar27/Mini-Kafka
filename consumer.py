import socket
import json
import threading
from time import sleep
import sys
import random
from datetime import datetime

IP = socket.gethostbyname(socket.gethostname())

Z_PORT = 9090
Z_ADDR = (IP, Z_PORT)

# PORT = 5566
# ADDR = (IP, PORT)

SIZE = 1024
FORMAT = "utf-8"
DISCONNECT_MSG = "!DISCONNECT"


topic = sys.argv[1]
check = sys.argv[2]

flag = 0
if(check == "--from-beginning"):
    flag = 1

def main():
    timestamp1= str((datetime.now().strftime('%H:%M:%S')))
    
    def broker(port_no):
        ADDR = (IP, port_no)
        client2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client2.connect(ADDR)
        print(f"[CONNECTED] Consumer connected to broker at {IP}:{port_no}")

        connected = True
        while connected:
            

            # data = {topic:msg}

            # data = json.dumps(data) #serialising
        
            message = "2"
            client2.send(message.encode())

            if message == DISCONNECT_MSG:
                connected = False
            else:
                msg = client2.recv(SIZE).decode(FORMAT)
                print(f"[BROKER] {msg}")

            print("check4")
            #timestamp1= str((datetime.now().strftime('%H:%M:%S')))
            dict3 = {topic:[timestamp1,flag]}
            print(dict3)
            print(type(dict3))
            dict3 = json.dumps(dict3)
            # y = "Hi"
            print("check3")
            client2.send(dict3.encode())
            print("check2")
            if msg == DISCONNECT_MSG:
                connected = False
            else:
                print("check")
                msg = client2.recv(SIZE).decode(FORMAT)
                msg = json.loads(msg)
                print(f"[BROKER] {msg}")
            sleep(10)
            zookeeper()
#--------------------------------------------------------------------------------------

# access details.txt to find leader and receive leader port number - use that and connect to it


    def zookeeper():
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(Z_ADDR)
        print(f"[CONNECTED] Consumer connected to zookeeper at {IP}:{Z_PORT}")

        connected = True
        while connected:
            
            message = "2"
            client.send(message.encode())

            if message == DISCONNECT_MSG:
                connected = False
            else:
                #message = client.recv(SIZE).decode(FORMAT)
                print("Waiting for reply")
                msg = client.recv(SIZE).decode(FORMAT)
                # data = json.loads(data)
                print(f"[Zookeeper] {msg}")


            if message == DISCONNECT_MSG:
                connected = False
            else:
                # message = client.recv(SIZE).decode(FORMAT)
                # # print("Waiting for reply2")
                data = client.recv(SIZE).decode(FORMAT)
                data = json.loads(data)
                print(f"[Zookeeper] {data}")

            l = []
            for key, value in data.items():
                l.append(int(value))

            
            rand = random.randint(0, len(l)-1)
            broker(l[0], )

            #connected = False
        
        #client.send(data.encode())
  
    #---------------------------------------------------------------------------------------------------
    

    thread1 = threading.Thread(target=zookeeper, args=())
    thread1.start()
    #print(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 1}")
    # thread2 = threading.Thread(target=broker, args=())
    # thread2.start()

if __name__ == "__main__":
    main()