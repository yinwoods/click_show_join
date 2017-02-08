import os
import time
import subprocess
from datetime import datetime,timedelta

def fileExist(latestTime):
    print(latestTime)
    command = 'hdfs dfs -test -e wasb://niphdid@nipspark.blob.core.windows.net/user/zhangrn/click_show_join/{date}/2300/_SUCCESS'.format(date=date)
    print(command)
    if subprocess.call(command, shell=True):
        print('file does not exist yet, wait 1 hour.')
        time.sleep(3600)
    else:
        print('start executing hive')
        command = 'hive -hiveconf date={date} -hiveconf hour={hour} -f getData.sql'.format(date=date, hour=hour)
        p = subprocess.Popen(command, shell=True)
        p.wait()


def getLatestHour():
    destpos = os.path.join(os.getcwd(), 'data')
    print(os.listdir(destpos))
    date = max(os.listdir(destpos))
    path = os.path.join(destpos, date)
    hour = max(os.listdir(path))
    year = int(date[0:4])
    month = int(date[4:6])
    day = int(date[6:8])
    hour = int(hour[:2])
    latestTime = datetime(year, month, day, hour)
    print(latestTime)
    now = latestTime + timedelta(hours=1)
    return now


while True:
    now = getLatestHour()
    latestTime = now.strftime('%Y%m%d')
    print(latestTime)
    fileExist(latestTime)
