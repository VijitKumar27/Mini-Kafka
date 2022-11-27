#import broker1

# def foo(res):
#     print(res)

import json
import socket


def request():
    print("enter topic name:")
    topic=input()
    broker1.accept(topic)

if __name__ == "__main__":
        
    while(1):
        request()