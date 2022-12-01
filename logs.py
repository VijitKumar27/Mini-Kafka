import os
import time
from datetime import datetime


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