import os
import time
import arrow
import subprocess


def fileExist(latestTime):
    """
    param: latestTIme 判断下一份拼接日志是否存在
    """
    date = latestTime.format('YYYYMMDD')

    command = ('hdfs dfs -test -e wasb://niphdme@nipspark.blob.core.windows.'
               'net/user/zhangrn/click_show_join/{date}/2300/_SUCCESS'
               ).format(date=date)

    print(command)
    if subprocess.call(command, shell=True):
        print('file does not exist yet, wait 20 hour.')
        time.sleep(72000)
    else:
        print('start executing hive')
        print('get {date}'.format(date=date))
        command = ('hive -hiveconf date={date} -f getData.sql'
                   ).format(date=date)
        p = subprocess.Popen(command, shell=True)
        p.wait()


def getLatestHour():
    """
    return: 返回data目录下的最新日期+1，也即下一份日志对应的日期
    """
    destpos = os.path.join(os.getcwd(), 'data')
    date = max(os.listdir(destpos))
    year = int(date[0:4])
    month = int(date[4:6])
    day = int(date[6:8])
    latestTime = arrow.get(year, month, day, 0, 0, 0)
    now = latestTime.replace(days=+1)
    return now


while True:
    latestTime = getLatestHour()
    fileExist(latestTime)
