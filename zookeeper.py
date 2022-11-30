import socket
import threading
#import wikipedia
import json
import os
from time import sleep

IP = socket.gethostbyname(socket.gethostname())
PORT = 9090
ADDR = (IP, PORT)
SIZE = 1024
FORMAT = "utf-8"
DISCONNECT_MSG = "!DISCONNECT"

d = {1: [5566,0], 2:[6677,1], 3: [7788,0]}

def handle_producer(conn, addr):
    for key, items in d.items():
            if(d[key][1] == 1):
                msg = d[key][0]   
                id = key
    msg = str(msg)
    conn.send(msg.encode(FORMAT))
    

def handle_consumer(conn, addr):
    l = []
    for key, items in d.items():
        if((d[key][1] == 1) or (d[key][1] == 0)):
            l.append(d[key][0])   
                
    msg = json.dump(l)
    conn.send(msg.encode(FORMAT))

def handle_broker(conn, addr):

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1)                                      #2 Second Timeout
    result = sock.connect_ex(('192.168.56.1',6677))
    if result == 0:
        print ('port OPEN')
    else:
        print ('port CLOSED, connect_ex returned: '+str(result))
    

    
  

def handle_client(conn, addr):
    print(f"[NEW CONNECTION] {addr} connected.")

    connected = True
    while connected:
        check = conn.recv(SIZE).decode(FORMAT)
        
        if(check == "1"):
            handle_producer(conn, addr)

        if(check == "2"):
            handle_consumer(conn, addr)

        if(check == "3"):
            handle_broker(conn, addr)
    conn.close()


def appoint_leader(id):
    if(id==3):
        print("No more brokers left, drop an F\n")
        return -1
    
    d[id][1] = -1 #making old leader dead
    d[id+1][1] = 1 #new leader
    return 1

def check_status():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)    
    
    for key, items in d.items():
            if(d[key][1] == 1):
                port_no = d[key][0]   
                id = key         
                          #2 Second Timeout
    # z = 1
    # while z!=-1:
    result = sock.connect_ex(('192.168.1.6',port_no))
    #print(result)
    #z = z + 1
    if result == 0:
        print ('port OPEN', port_no)
        #z = -1   
    else:
        print ('port CLOSED, connect_ex returned: '+str(result))
        z = appoint_leader(id)
            
    sleep(1)
    check_status()
    


def main():

    thread2 = threading.Thread(target=check_status, args=())
    thread2.start()
    

    print("[STARTING] Zookeeper is starting...")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)
    server.listen()
    print(f"[LISTENING] Zookeeper is listening on {IP}:{PORT}")

    while True:
        conn, addr = server.accept()
        
        
        # thread2 = threading.Thread(target=handle_broker, args=(conn, addr))
        # thread2.start()

        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()

        print(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 1}")

if __name__ == "__main__":
    main()






