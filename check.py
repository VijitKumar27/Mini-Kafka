import json

d = {"1": [5566,1], "2":[6677,0], "3": [7788,0]}
data = json.dumps(d)

with open('details.txt','w') as f:
    f.write(data)

with open('details.txt','r') as f1:
    z = f1.read()
    z = json.loads(z)

    for key, items in z.items():
        if(z[key][1] == 1):
            final_port = z[key][0]
            break

print(final_port)



