# coding: utf-8
import re
import os
from config import mssqlconfig_online, LAST_PUT_TIME_FILE_HIS
import pymssql
import time
from datetime import datetime
from datetime import timedelta


def query(sql):
    with pymssql.connect(**mssqlconfig_online) as conn:
        with conn.cursor(as_dict=True) as cursor:
            res = cursor.execute(sql)
            res = cursor.fetchall()
    return res


def getlasttime():
    lasttime = None
    if os.path.exists(LAST_PUT_TIME_FILE_HIS):
        with open(LAST_PUT_TIME_FILE_HIS) as timefile:
            lasttime = timefile.readlines()[-1]
    return lasttime


def updatelasttime(lasttime):
    try:
        with open(LAST_PUT_TIME_FILE_HIS, 'w+') as timefile:
            timefile.write(lasttime + '\n')
        return True
    except:
        return False


def databyhourtask(end_stamp):
    # try:
        start_stamp = end_stamp - timedelta(hours=1)
        starttime_string = datetime.strftime(start_stamp, '%Y-%m-%d %H:%M')
        endtime_string = datetime.strftime(end_stamp, '%Y-%m-%d %H:%M')
        sql = ("select NewsId, [Like], UserId from "
               "NewsLikes where Timestamp>='%s' and "
               "Timestamp<'%s'") % (starttime_string, endtime_string)
        print(sql)
        data = query(sql)
        filename = re.sub('-| |:', '', starttime_string)
        filedate = filename[:8]
        filepath = './data/%s/%s' % (filedate, filename)
        if os.path.exists(filepath):
            os.remove(filepath)
        if os.path.exists('./data/%s' % filedate) is False:
            os.mkdir('./data/%s' % filedate)
        with open(filepath, 'wt') as fh:
            for dt in data:
                fh.write('%s\t%d\t%d\t%d\n' % (dt['UserId'], dt['NewsId'], 1
                         if dt['Like'] == 1
                         else 0,  1 if dt['Like'] == -1 else 0))
        os.system("bash ./puthdfs.sh %s %s" % (filedate, filename))
        updatelasttime(starttime_string)
        return True
    # except:
        # return False


def main():
    end_ti = '2017-02-03 03:00'
    end_ti_stamp = datetime.strptime(end_ti, '%Y-%m-%d %H:%M')
    starttime = '2017-02-01 00:00'
    starttime_stamp = datetime.strptime(starttime, '%Y-%m-%d %H:%M')
    while True:

        if end_ti_stamp >= starttime_stamp:
            res = databyhourtask(end_ti_stamp)
            if res is True:
                end_ti_stamp -= timedelta(hours=1)
                print('%s %s' % (time.ctime(), '%s ok, wait' % end_ti_stamp))

            else:
                print('%s %s' % (time.ctime(), 'error'))
                break
        else:
            break
            print('ok, break')

if __name__ == "__main__":
    main()
