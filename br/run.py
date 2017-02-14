import time
import arrow
import subprocess
from config import IMPRESSION_PATH
from config import CLICK_PATH
from config import DIANZAN_PATH
from config import JOIN_LOG_PATH


def impression_log_exist(times):
    for time in times:
        date = time.format('YYYYMMDD')
        hour = time.format('HH')
        command = '/usr/bin/hadoop fs -test -e '
        command += IMPRESSION_PATH.format(date, hour)
        statu = subprocess.call(command, shell=True)
        print(command)
        if statu == 1:
            print('impression log with {}/{} not ready'.format(date, hour))
            return 1
    return 0


def click_log_exist(times):
    for time in times:
        date = time.format('YYYYMMDD')
        hour = time.format('HH')
        command = '/usr/bin/hadoop fs -test -e '
        command += CLICK_PATH.format(date, hour)
        print(command)
        statu = subprocess.call(command, shell=True)
        if statu == 1:
            print('click log with {}/{} not ready'.format(date, hour))
            return 1
    return 0


def dianzan_log_exist(times):
    for time in times:
        date = time.format('YYYYMMDD')
        hour = time.format('HH')
        command = '/usr/bin/hadoop fs -test -e '
        command += DIANZAN_PATH.format(date, hour)
        print(command)
        statu = subprocess.call(command, shell=True)
        if statu == 1:
            print('dianzan log with {}/{} not ready'.format(date, hour))
            return 1
    return 0


def main():

    utc_br = arrow.utcnow().replace(hours=-3)
    utc_br = arrow.get(2017, 2, 7, 16, 0, 0)

    while True:

        hour_left1 = utc_br.replace(hours=-1)
        hour_left2 = utc_br.replace(hours=-2)
        hour_left3 = utc_br.replace(hours=-3)
        hour_left4 = utc_br.replace(hours=-4)
        hour_left5 = utc_br.replace(hours=-5)
        hour_left8 = utc_br.replace(hours=-8)
        hour_left9 = utc_br.replace(hours=-9)
        hour_left10 = utc_br.replace(hours=-10)
        hour_left11 = utc_br.replace(hours=-11)
        hour_left12 = utc_br.replace(hours=-12)
        date = hour_left4.format('YYYYMMDD')

        # judge if files needed exist
        impression_statu = impression_log_exist([hour_left4])

        click_statu = click_log_exist(
                [hour_left5, hour_left4, hour_left3, hour_left2, hour_left1]
            )

        dianzan_statu = dianzan_log_exist(
                [hour_left12, hour_left11, hour_left10, hour_left9, hour_left8]
            )

        if any([impression_statu, click_statu, dianzan_statu]):
            print('wait 30 minutes')
            time.sleep(1800)
        else:
            print('start to execute hive sql')
            command = 'hive\
                       -hiveconf date={date}\
                       -hiveconf hour_left1={hour_left1}\
                       -hiveconf hour_left2={hour_left2}\
                       -hiveconf hour_left3={hour_left3}\
                       -hiveconf hour_left4={hour_left4}\
                       -hiveconf hour_left5={hour_left5}\
                       -hiveconf hour_left8={hour_left8}\
                       -hiveconf hour_left9={hour_left9}\
                       -hiveconf hour_left10={hour_left10}\
                       -hiveconf hour_left11={hour_left11}\
                       -hiveconf hour_left12={hour_left12}\
                       -f getData.sql'.format(
                            date=date,
                            hour_left1=hour_left1.format('HH'),
                            hour_left2=hour_left2.format('HH'),
                            hour_left3=hour_left3.format('HH'),
                            hour_left4=hour_left4.format('HH'),
                            hour_left5=hour_left5.format('HH'),
                            hour_left8=hour_left8.format('HH'),
                            hour_left9=hour_left9.format('HH'),
                            hour_left10=hour_left10.format('HH'),
                            hour_left11=hour_left11.format('HH'),
                            hour_left12=hour_left12.format('HH'),
                       )
            subprocess.call(command, shell=True)
            utc_br = utc_br.replace(hours=+1)


if __name__ == '__main__':
    main()
