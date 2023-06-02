from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


import os
import pandas as pd

def upload():
    gauth = GoogleAuth()           
    drive = GoogleDrive(gauth)  
    f = open("/Users/guyrawit/data_project/filename.txt")
    file = f.readline()
    a = os.listdir(file)
    csv = ''
    for i in a:
        if i[0:4] == "part":
            csv = i
    f = file + '/' + csv
    print(f)
    upload_file_list = [f]
    id = "1TXJ_yvaB-UXkz3K3z8Lxn0WYRQKOmZoz"
    for upload_file in upload_file_list:
        gfile = drive.CreateFile({'parents': [{'id': id}]})
        # Read file and set it as the content of this instance.
        gfile.SetContentFile(upload_file)
        gfile.Upload() # Upload the file.
    return 0 
