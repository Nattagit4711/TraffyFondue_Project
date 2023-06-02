import requests
import json
import pandas as pd
import os 
from datetime import datetime

def extract_data():
    #define last cursor id from exist csv
    f= open("/Users/guyrawit/data_project/checkpoint.txt", "r")
    date = f.readline()
    last_df = pd.read_csv('/Users/guyrawit/data_project/{}'.format(date))
    date_format = '%y-%m-%d %H:%M:%S'
    last_cursor_time = datetime.strptime(last_df.iloc[0]["timestamp"][2:19], date_format)

    #define some variables
    list_of_row = []
    count = 0
    print("last time stamp = {}".format(last_cursor_time))
    while True:
        path = "https://publicapi.traffy.in.th/share/teamchadchart/search?limit=100&offset={}".format(count*100)    
        response = requests.get(path).text
        response_info = json.loads(response)
        new_cursor_time = datetime.strptime(response_info['results'][99]['timestamp'][2:19], date_format)
        new_cursor_time2 = datetime.strptime(response_info['results'][0]['timestamp'][2:19], date_format)
        if last_cursor_time < new_cursor_time:
            list_of_row+= response_info['results']
            count += 1
            print("Added {} rows to the list".format(count*100))
            print("new cursor timestamp = {}".format(new_cursor_time, last_cursor_time))
        else :
            for row in response_info['results']:
                if datetime.strptime(row['timestamp'][2:19], date_format) > last_cursor_time :
                    list_of_row.append(row)
                else:
                    break
            break

    df = pd.DataFrame(list_of_row)
    time = datetime.now().strftime("%x-%X")

    df.to_csv("/Users/guyrawit/data_project/traffy-{}.csv".format(str(datetime.now())[:10]))

    print("\"traffy-{}.csv\" has been created !".format(str(datetime.now())[:10]))
    f = open("/Users/guyrawit/data_project/checkpoint.txt", "w")
    f.write("traffy-{}.csv".format(str(datetime.now())[:10]))
    return 0 