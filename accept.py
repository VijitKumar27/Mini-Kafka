import os

def accept(topic):  #we get the request here
    with open('topics.txt','a+') as f: #global list of topics
        f.write(str([]))
        lst=f.read()
        lst=list(lst)
        if topic not in lst:
            lst.append(topic)
        f.write(str(lst))
    if not os.path.exists(topic):
        os.mkdir(topic)

    x=topic+"/d"
    with open(x, 'a') as f:
        f.write('')
    with open(x, 'r') as f:
        res=f.read()
    consumer.foo(res)