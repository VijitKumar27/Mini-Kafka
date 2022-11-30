import subprocess as sp
#import pyping
import os
import ping3
from pythonping import ping


d = {1: [5566,1], 2:[6677,0], 3: [7788,0]}

def appoint_leader(id):
    if(id==3):
        print("No more brokers left, drop an F\n")
        return -1
    
    d[id][1] = -1 #making old leader dead
    d[id+1][1] = 1 #new leader
    return 1

def check_leader_status(id):
    port_no = d[id][0]
    ip = "192.168.1.6:" +  str(port_no)
    

   
    # ping_result = ping(target=ip, count=10, timeout=2)

    # return {
    #     'host': ip,
    #     'avg_latency': ping_result.rtt_avg_ms,
    #     'min_latency': ping_result.rtt_min_ms,
    #     'max_latency': ping_result.rtt_max_ms,
    #     'packet_loss': ping_result.packet_loss
    # }

    

    
    #hostname = "google.com" #example
    # response = os.system("ping " + ip)

    # #and then check the response...
    # if response == 0:
    #     print(ip, 'is up!')
    # else:
    #     print(ip, 'is down!')
    #     x = appoint_leader(id)
    #     return x

    status,result = sp.getstatusoutput("ping -a " + ip)
    print(result)

    if status == 0: #and int(result[-1]) == 1:
        print("System " + ip + " is UP !")
        
    elif status == 1: #and int(result[-1]) == 0:
        print("System " + ip + " is DOWN !")
        x = appoint_leader(id)
        return x

#     status, output = sp.getstatusoutput("ping -c1 -w1 " + ip)

# if status == 0 and int(output[-1]) == 1:
#     print("System " + ip + " is UP !")
# elif status == 0 and int(output[-1]) == 0:
#     print("OK: innodb_file_per_table is disabled!")
    

def main():

    working = True
    while working:
        for key, items in d.items():
            if(d[key][1] == 1):
                x = key
        y = check_leader_status(x)

        if(y == -1):
            working = False
    
    print(d)




if __name__ == "__main__":
    main()


# '''
# def isUp(hostname):

#     giveFeedback = False

#     if platform.system() == "Windows":
#         response = os.system("ping "+hostname+" -n 1")
#     else:
#         response = os.system("ping -c 1 " + hostname)

#     isUpBool = False
#     if response == 0:
#         if giveFeedback:
#             print hostname, 'is up!'
#         isUpBool = True
#     else:
#         if giveFeedback:
#             print hostname, 'is down!'

#     return isUpBool

# '''

# '''''''''
# ----------------------------------------
# import subprocess
# import platform

# operating_sys = platform.system()
# nas = '192.168.0.10'

# def ping(ip):
#     # ping_command = ['ping', ip, '-n', '1'] instead of ping_command = ['ping', ip, '-n 1'] for Windows
#     ping_command = ['ping', ip, '-n', '1'] if operating_sys == 'Windows' else ['ping', ip, '-c 1']
#     shell_needed = True if operating_sys == 'Windows' else False

#     ping_output = subprocess.run(ping_command,shell=shell_needed,stdout=subprocess.PIPE)
#     success = ping_output.returncode
#     return True if success == 0 else False

# out = ping(nas)
# print(out)''''''
# # --------------------------------------------------------------------------------------------------

# '''


# ''''''''''
# def ping_server(server: str, port: int, timeout=3):
#     """ping server"""
#     try:
#         socket.setdefaulttimeout(timeout)
#         s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#         s.connect((server, port))
#     except OSError as error:
#         return False
#     else:
#         s.close()
#         return True
# '''''

# import rpyc
# from time import sleep

# c = rpyc.connect("127.0.0.1", 7788)
# while True:
#    try:
#         c.root.get_heartbeat()
#         sleep(5)
#    except Exception:
#         print("Error")

     # sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
     # sock.settimeout(2)                                      #2 Second Timeout
     # result = sock.connect_ex(('192.168.56.1',5566))
     # if result == 0:
     #    print ('port OPEN')
     # else:
     #    print ('port CLOSED, connect_ex returned: '+str(result))