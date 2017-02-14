import time
import arrow
import subprocess
from config import IMPRESSION_PATH
from config import CLICK_PATH
from config import DIANZAN_PATH


def judge_impression_exist(date, hours):
    for hour in hours:
        command = IMPRESSION_PATH.format(hour)
        statu = subprocess.call(command)
        if statu == 0:
            print('impression log with {} not ready'.format(hour))
            return 0
    return 1


def judge_click_exist(date, hours):
    for hour in hours:
        command = CLICK_PATH.format(hour)
        statu = subprocess.call(command)
        if statu == 0:
            print('click log with {} not ready'.format(hour))
            return 0
    return 1


def judge_dianzan_exist(date, hours):
    for hour in hours:
        command = DIANZAN_PATH.format(hour)
        statu = subprocess.call(command)
        if statu == 0:
            print('dianzan log with {} not ready'.format(hour))
            return 0
    return 1


def main():

    while True:

        time_ = arrow.now()
        date = time_.format('YYYYMMDD')
        hour_left1 = time_.replace(hours=-1).format('HH')
        hour_left2 = time_.replace(hours=-2).format('HH')
        hour_left3 = time_.replace(hours=-3).format('HH')
        hour_left4 = time_.replace(hours=-4).format('HH')
        hour_left5 = time_.replace(hours=-5).format('HH')
        hour_left8 = time_.replace(hours=-8).format('HH')
        hour_left9 = time_.replace(hours=-9).format('HH')
        hour_left10 = time_.replace(hours=-10).format('HH')
        hour_left11 = time_.replace(hours=-11).format('HH')
        hour_left12 = time_.replace(hours=-12).format('HH')

        # judge if files needed exist
        impression_statu = judge_impression_exist(date, [hour_left4])

        click_statu = judge_click_exist(
                date,
                [hour_left5, hour_left4, hour_left3, hour_left2, hour_left1]
            )

        dianzan_statu = judge_dianzan_exist(
                date,
                [hour_left12, hour_left11, hour_left10, hour_left9, hour_left8]
            )

        if all([impression_statu, click_statu, dianzan_statu]):
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
                            hour_left1=hour_left1,
                            hour_left2=hour_left2,
                            hour_left3=hour_left3,
                            hour_left4=hour_left4,
                            hour_left5=hour_left5,
                            hour_left8=hour_left8,
                            hour_left9=hour_left9,
                            hour_left10=hour_left10,
                            hour_left11=hour_left11,
                            hour_left12=hour_left12,
                       )
            subprocess.call(command)

        else:
            time.sleep(1800)


if __name__ == '__main__':
    main()
