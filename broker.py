import socket
import threading
#import wikipedia
import json
import os
import shutil

IP = socket.gethostbyname(socket.gethostname())
PORT = 5566
ADDR = (IP, PORT)
SIZE = 1024
FORMAT = "utf-8"
DISCONNECT_MSG = "!DISCONNECT"

def handle_producer(conn, addr):

    msg = "I reached publisher function"
    conn.send(msg.encode(FORMAT))

    data = conn.recv(SIZE).decode(FORMAT)
    data = json.loads(data)
        # if msg == DISCONNECT_MSG:
        #     connected = False
    msg2 = "Published successfully"
    conn.send(msg2.encode(FORMAT))

    print(f"[{addr}] {data}")
    

    for item in data.keys():

        #check if topic exists, only then write topic
        with open('topics.txt','a+') as f: #global list of topics
            f.write(str([]))
            lst=f.read()
            lst=list(lst)
            if item not in lst:
                lst.append(item)
            f.write(str(lst))
        
            # os.mkdir("Topics")
            # y = "Topics/" + item
            if not os.path.exists('brokera/'+item):
                os.mkdir('brokera/'+item)
            ##os.mkdir(item)

    for key, value in data.items():
        x="brokera/"+key+"/d"
        with open(x, 'a') as f:
            f.write(str(value))
    
    #REPLICATION
    if os.path.exists('brokerb'):
        shutil.rmtree('brokerb')
    if os.path.exists('brokerc'):
        shutil.rmtree('brokerc')

    src=r"brokera"
    dest1=r"brokerb"
    dest2=r"brokerc"
    
    shutil.copytree(src,dest1)
    shutil.copytree(src,dest2)   



def handle_consumer(conn, addr):
    
    msg = "I reached consumer function"
    conn.send(msg.encode(FORMAT))

    topic = conn.recv(SIZE).decode(FORMAT)
    print(f"[{addr}] {topic}")
    
    with open('topics.txt','a+') as f: #global list of topics
        f.write(str([]))
        lst=f.read()
        lst=list(lst)
        if topic not in lst:
            lst.append(topic)
        f.write(str(lst))
    #if topic doesnt exist, create one
    if not os.path.exists("brokera/"+topic):
        os.mkdir("brokera/"+topic)
        x="brokera/"+topic+"/d"
        value=" "
        with open(x, 'a') as f:
            f.write(value)
    

        #REPLICATION
        if os.path.exists('brokerb'):
            shutil.rmtree('brokerb')
        if os.path.exists('brokerc'):
            shutil.rmtree('brokerc')

        src=r"brokera"
        dest1=r"brokerb"
        dest2=r"brokerc"

        shutil.copytree(src,dest1)
        shutil.copytree(src,dest2) 


        
    x="brokera/"+topic+"/d"
    with open(x, 'a') as f:
        f.write('')
    with open(x, 'r') as f:
        res=f.read()
        conn.send(res.encode(FORMAT))
            

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
    print("[STARTING] Server is starting...")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)
    server.listen()
    print(f"[LISTENING] Server is listening on {IP}:{PORT}")

    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 1}")

if __name__ == "__main__":
    main()
