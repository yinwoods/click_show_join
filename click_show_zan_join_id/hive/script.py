import os
import time
import arrow
import subprocess

def fileExist(latestTime):
    date = latestTime.format('YYYYMMDD')
    hour = latestTime.format('HH')
    command = 'hdfs dfs -test -e wasb://niphdid@nipspark.blob.core.windows.net/user/zhangrn/click_show_join/{date}/{hour}00/_SUCCESS'.format(date=date, hour=hour)
    print(command)
    if subprocess.call(command, shell=True):
        print('file does not exist yet, wait 1 hour.')
        time.sleep(3600)
    else:
        print('start executing hive')
        print('get {date}/{hour}'.format(date=date, hour=hour))
        command = 'hive -hiveconf date={date} -hiveconf hour={hour}00 -f getData.sql'.format(date=date, hour=hour)
        p = subprocess.Popen(command, shell=True)
        p.wait()


def getLatestHour():
    destpos = os.path.join(os.getcwd(), 'data')
    date = max(os.listdir(destpos))
    path = os.path.join(destpos, date)
    hour = max(os.listdir(path))
    year = int(date[0:4])
    month = int(date[4:6])
    day = int(date[6:8])
    hour = int(hour[:2])
    latestTime = arrow.get(year, month, day, hour)
    now = latestTime.replace(hours=+1)
    return now


while True:
    latestTime = getLatestHour()
    fileExist(latestTime)
