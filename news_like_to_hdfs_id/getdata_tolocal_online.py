# coding: utf-8
import os
import re
import pymssql
import time
from config import mssqlconfig_online, LAST_PUT_TIME_FILE


def query(sql):
    with pymssql.connect(**mssqlconfig_online) as conn:
        with conn.cursor(as_dict=True) as cursor:
            res = cursor.execute(sql)
            res = cursor.fetchall()
    return res


def getlasttime():
    lasttime = None
    if os.path.exists(LAST_PUT_TIME_FILE):
        with open(LAST_PUT_TIME_FILE) as timefile:
            lasttime = timefile.readlines()[-1]
    return lasttime


def updatelasttime(lasttime):
    try:
        with open(LAST_PUT_TIME_FILE, 'w+') as timefile:
            timefile.write(lasttime + '\n')
        return True
    except:
        return False


def databyhourtask(one_hour_ago, ti):
    # try:
        sql = ("select NewsId, [Like], UserId "
               "from NewsLikes where Timestamp>='%s' "
               "and Timestamp<'%s'") % (one_hour_ago, ti)
        print(sql)
        data = query(sql)
        filename = re.sub('-| |:', '', one_hour_ago)
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
        return True


def main():
    while True:
        sql = ("select top 1 Timestamp as ti from "
               "NewsLikes order by Timestamp desc")
        newest_news = query(sql)
        ti = newest_news[0]['ti']
        ti = ti[:13] + ':00'
        lasttime = getlasttime()
        if lasttime is None:
            lasttime_stamp = 0
        else:
            print(lasttime)
            lasttime_stamp = int(time.mktime(time.strptime(lasttime.strip(),
                                 '%Y-%m-%d %H:%M')))

        ti_stamp = int(time.mktime(time.strptime(ti, '%Y-%m-%d %H:%M')))
        if ti_stamp - lasttime_stamp > 3600:
            one_hour_ago = time.strftime(
                    "%Y-%m-%d %H:%M", time.localtime(ti_stamp - 3600))
            res = databyhourtask(one_hour_ago, ti)
            if res is True:
                updatelasttime(one_hour_ago,)
                print('%s %s' % (time.ctime(), '%s ok, wait' % one_hour_ago))
                time.sleep(5 * 60)

            else:
                print('%s %s' % (time.ctime(), 'error'))
                break
        else:
            print('%s %s' % (time.ctime(), 'wait'))
            time.sleep(5 * 60)

if __name__ == "__main__":
    main()
