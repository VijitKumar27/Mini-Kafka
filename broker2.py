import socket
import threading
#import wikipedia
import json
import os
import shutil
import random
from datetime import datetime
import time
from collections import OrderedDict


IP = socket.gethostbyname(socket.gethostname())
PORT = 6677
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
        #partitoning

        #check if topic exists, only then write topic
        # with open('topics.txt','a+') as f: #global list of topics
        #     f.write(str([]))
        #     lst=f.read()
        #     lst=list(lst)
        #     if item not in lst:
        #         lst.append(item)
        #     f.write(str(lst))
        
            # os.mkdir("Topics")
            # y = "Topics/" + item
        #PARTITIONING
        if not os.path.exists('brokera/'+item):
            os.mkdir('brokera/'+item)
            f1=open('brokera/'+item+'/d1','w')
            f2=open('brokera/'+item+'/d2','w')
            f3=open('brokera/'+item+'/d3','w')

    dict1 ={}
####################
    for key, value in data.items():
        i=random.randint(1,3)
        

        x='brokera/'+key+'/d'+str(i)
        print(x)
        with open(x, 'r') as f:
            print("inside with1")
            v=f.read()
            print(v)
            if(v==''):
                pass
            else:
                dict1=json.loads(v)
                timestamp=str((datetime.now().strftime('%H:%M:%S')))
                dict1[timestamp]=value
            print(dict1)
        with open(x,'w') as f:
            if(v==''):
                timestamp=str((datetime.now().strftime('%H:%M:%S')))
                dict1[timestamp]=value
                y=json.dumps(dict1)
            else:
                print("inside with2")
                y=json.dumps(dict1)
            f.write(y)
    

    #REPLICATION
    if os.path.exists('brokerb'):
        shutil.rmtree('brokerb')

    src=r"brokera"
    dest1=r"brokerb"
    
    shutil.copytree(src,dest1)
      



def handle_consumer(conn, addr):
    
    msg = "I reached consumer function"
    conn.send(msg.encode(FORMAT))

    detail = conn.recv(SIZE).decode(FORMAT)
    detail = json.loads(detail)
    print("hi")
    # msg = "OK"
    # conn.send(msg.encode(FORMAT))

    print(f"[{addr}] {detail}")
    
    for key, value in detail.items():
        topic = key
        flag = value[1]
        timestamp1 = value[0]
    
    if (flag == 1): 
        # if not os.path.exists("brokera/"+topic):
        #     os.mkdir("brokera/"+topic)
        #     x="brokera/"+topic+"/d"+str(i)
        #     value=" "
        #     with open(x, 'a') as f:
        #         f.write(value)
        print("entered")
        if not os.path.exists('brokera/'+topic):
            print("entered if")
            os.mkdir('brokera/'+topic)
            f1=open('brokera/'+topic+'/d1','w') 
            f2=open('brokera/'+topic+'/d2','w')
            f3=open('brokera/'+topic+'/d3','w')

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

        dict1={}
        for i in range(1,4):
            print("entered for")
            x="brokera/"+topic+"/d"+str(i)
            # with open(x, 'a') as f:
            #     f.write('')
            with open(x, 'r') as f:
                print("entered with")
                print("before")
                res=f.read()
                if (res==''):
                    pass
                    print("here")
                else: 
                    print("there")  
                #print(type(res))
                    y=json.loads(res)
                    print(y)
                    dict1.update(y)
        dict1 = json.dumps(dict1)
        conn.send(dict1.encode(FORMAT))

    else:
        # if not os.path.exists("brokera/"+topic):
        #     os.mkdir("brokera/"+topic)
        #     x="brokera/"+topic+"/d"+str(i)
        #     value=" "
        #     with open(x, 'a') as f:
        #         f.write(value)
        print("entered")
        if not os.path.exists('brokera/'+topic):
            print("entered if")
            os.mkdir('brokera/'+topic)
            f1=open('brokera/'+topic+'/d1','w') 
            f2=open('brokera/'+topic+'/d2','w')
            f3=open('brokera/'+topic+'/d3','w')

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

        dict1={}
        for i in range(1,4):
            print("entered for")
            x="brokera/"+topic+"/d"+str(i)
            # with open(x, 'a') as f:
            #     f.write('')
            with open(x, 'r') as f:
                print("entered with")
                print("before")
                res=f.read()
                if (res==''):
                    pass
                    print("here")
                else: 
                    print("there")  
                #print(type(res))
                    y=json.loads(res)
                    print(y)
                    dict1.update(y)


    
        dict2 = OrderedDict(sorted(dict1.items()))
        #dict4 = dict2
        # print(dict2)
        #dict1 = dict(sorted(key=lambda x:time.mktime(time.strptime(x['timestamp'], '%H:%M:%S'))))
        #dict1= dict(sorted(final.items(), key=lambda item: item[0])) 
        #print(dict2)
        #timestamp1= str((datetime.now().strftime('%H:%M:%S')))
        dict3 = {}
        for key, value in dict2.items():
            if key>timestamp1:
                dict3[key] = value

        print(dict3)
        dict4 = json.dumps(dict3)
        conn.send(dict4.encode(FORMAT))

        # else:
        #     timestamp=str((datetime.now().strftime('%H:%M:%S')))
        #     for file in os.listdir('brokera'):
                




            
def logs():
    path="/"

    obj=os.scandir()

    print("Files and Directories in '%s'" % path)

    for entry in obj:
        if entry.is_dir():
        #print(entry.name)
            ti_c=os.path.getctime(entry.name)
            c_ti=time.ctime(ti_c)
            #print(c_ti)
            dict={}
            file1 = open("logs.txt", "a")
            dict[entry.name]=c_ti
            file1.write(str((datetime.now().strftime('%H:%M:%S'+'\n'))))
            file1.write(str(path+'\n'))
            file1.write(str(dict))
        #file1.write(str(producer.data))      

def handle_zookeeper(conn, addr):
    msg = "3"
    conn.send(msg.encode(FORMAT))
            

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
            handle_zookeeper(conn, addr)
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
        logs()

if __name__ == "__main__":
    main()
