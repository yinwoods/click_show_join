import time
import arrow
import subprocess
from config import IMPRESSION_PATH
from config import CLICK_PATH
from config import JOIN_LOG_SUCCESS_PATH


def impression_log_exist(times):
    """
    param：日期list
    return: 用 0 1 表示日志是否存在
    """
    for time_ in times:
        year = time_.format('YYYY')
        month = time_.format('MM')
        day = time_.format('DD')
        hour = time_.format('HH')
        command = '/usr/bin/hadoop fs -test -e '
        command += IMPRESSION_PATH.format(
                        year=year,
                        month=month,
                        day=day,
                        hour=hour)
        statu = subprocess.call(command, shell=True)
        print(command)
        if statu == 1:
            print('impression log with {year}/{month}/{day}/{hour} not ready'.format(
                    year=year,
                    month=month,
                    day=day,
                    hour=hour))
            return 1
    return 0


def click_log_exist(times):
    """
    param：日期list
    return: 用 0 1 表示日志是否存在
    """
    for time_ in times:
        year = time_.format('YYYY')
        month = time_.format('MM')
        day = time_.format('DD')
        hour = time_.format('HH')
        command = '/usr/bin/hadoop fs -test -e '
        command += CLICK_PATH.format(
                        year=year,
                        month=month,
                        day=day,
                        hour=hour)
        statu = subprocess.call(command, shell=True)
        print(command)
        if statu == 1:
            print('click log with {year}/{month}/{day}/{hour} not ready'.format(
                    year=year,
                    month=month,
                    day=day,
                    hour=hour))
            return 1
    return 0


def main():

    utc_id = arrow.utcnow().replace(hours=+7)
    utc_id = arrow.get(2017, 7, 9, 0, 0, 0)

    while True:
        hour_left1 = utc_id.replace(hours=-1)
        hour_left2 = utc_id.replace(hours=-2)
        hour_left3 = utc_id.replace(hours=-3)
        hour_left4 = utc_id.replace(hours=-4)
        hour_left5 = utc_id.replace(hours=-5)
        date = hour_left4.format('YYYYMMDD')
        year = hour_left4.format('YYYY')
        month = hour_left4.format('MM')
        day = hour_left4.format('DD')

        # judge if files needed exist
        impression_statu = impression_log_exist([hour_left4])

        click_statu = click_log_exist(
                [hour_left5, hour_left4, hour_left3, hour_left2, hour_left1]
            )

        if any([impression_statu, click_statu]):
            print('wait 30 minutes')
            time.sleep(1800)
        else:

            print('start to execute hive sql')
            command = 'hive\
                       -hiveconf year={year}\
                       -hiveconf month={month}\
                       -hiveconf day={day}\
                       -hiveconf date={date}\
                       -hiveconf hour_left1={hour_left1}\
                       -hiveconf hour_left2={hour_left2}\
                       -hiveconf hour_left3={hour_left3}\
                       -hiveconf hour_left4={hour_left4}\
                       -hiveconf hour_left5={hour_left5}\
                       -f getData.sql'.format(
                            year=year,
                            month=month,
                            day=day,
                            date=date,
                            hour_left1=hour_left1.format('HH'),
                            hour_left2=hour_left2.format('HH'),
                            hour_left3=hour_left3.format('HH'),
                            hour_left4=hour_left4.format('HH'),
                            hour_left5=hour_left5.format('HH'),
                       )
            subprocess.call(command, shell=True)

            # tag success
            command = "/usr/bin/hadoop fs -touchz " + JOIN_LOG_SUCCESS_PATH.format(date, hour_left4.format('HH'))
            print(command)
            subprocess.call(command, shell=True)

            utc_id = utc_id.replace(hours=+1)


if __name__ == '__main__':
    main()
