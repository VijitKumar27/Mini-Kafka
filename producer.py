#import broker1,broker2,broker3
import json
import socket

while(1):
    print("Enter topic name: ")
    topic = input()
    print("Enter the desired message:")
    msg = input()

    data = {topic:msg}


    data = json.dumps(data) #serialising

    clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
    clientSocket.connect(("127.0.0.1",9090));

    #print(data)

    clientSocket.send(data.encode());

#broker1.func(data)   

# dataFromServer = clientSocket.recv(1024);

# print(dataFromServer.decode());


