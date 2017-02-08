import os
import time
import subprocess
from datetime import datetime,timedelta

def fileExist(date):
    states = []
    command = 'hdfs dfs -test -e wasb://niphdbr@nipspark.blob.core.windows.net/user/zhangrn/click_show_join/{date}/2300/_SUCCESS'.format(date=date)
    print(command)
    if subprocess.call(command, shell=True):
        print('file does not exist yet, wait 1 hour.')
        time.sleep(3600)
    else:
        print('start executing hive')
        command = 'hive -hiveconf date={date} -f getData.sql'.format(date=date)
        p = subprocess.Popen(command, shell=True)
        p.wait()


def getLatestDate():
    destpos = os.path.join(os.getcwd(), 'data')
    date = max(os.listdir(destpos))
    path = os.path.join(destpos, date)
    year = int(date[0:4])
    month = int(date[4:6])
    day = int(date[6:8])
    latestDate = datetime(year, month, day)
    now = latestDate + timedelta(days=1)
    print(latestDate)
    return now

while True:
    now = getLatestDate()
    date = now.strftime('%Y%m%d')
    print(date)
    fileExist(date)
