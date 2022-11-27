import os
import sys
import consumer
import socket
import json

serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM);

serverSocket.bind(("127.0.0.1",9090));

serverSocket.listen();

while(True):

    (clientConnected, clientAddress) = serverSocket.accept();

    print("Accepted a connection request from %s:%s"%(clientAddress[0], clientAddress[1]));
    data = clientConnected.recv(1024)
    data = json.loads(data) #deserialisng
    #clientConnected.send("Topic published successfully".encode());

 
   
    #creating directories for each topic
    for item in data.keys():

        #check if topic exists, only then write topic
        with open('topics.txt','a+') as f: #global list of topics
            f.write(str([]))
            lst=f.read()
            lst=list(lst)
            if item not in lst:
                lst.append(item)
            f.write(str(lst))

        if not os.path.exists(item):
            os.mkdir(item)
        ##os.mkdir(item)

    for key, value in data.items():
        x=key+"/d"
        with open(x, 'a') as f:
            f.write(str(value))
    
        
# def accept(topic):  #we get the request here
#     with open('topics.txt','a+') as f: #global list of topics
#         f.write(str([]))
#         lst=f.read()
#         lst=list(lst)
#         if topic not in lst:
#             lst.append(topic)
#         f.write(str(lst))
#     if not os.path.exists(topic):
#         os.mkdir(topic)

#     x=topic+"/d"
#     with open(x, 'a') as f:
#         f.write('')
#     with open(x, 'r') as f:
#         res=f.read()
#     consumer.foo(res)