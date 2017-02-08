import sys
import subprocess


def monitor(host, log_name, log_date, log_update_freq):
    if log_update_freq == 'hourly':
        url = ('wasb://niphd{host}@nipspark.blob.core.windows.net'
               '/dailyCtr/{name}-{date}/*').format(
                host=host,
                name=log_name,
                date=log_date
            )
        command = 'hdfs dfs -ls {}'.format(url)
        subprocess.call(command, shell=True)
    elif log_update_freq == 'daily':
        url = ('wasb://niphd{host}@nipspark.blob.core.windows.net'
               '/dailyCtr/{name}-{date}').format(
                host=host,
                name=log_name,
                date=log_date
            )
        command = 'hdfs dfs -test -e {}'.format(url)
        status = subprocess.call(command, shell=True)
        if int(status) == 0:
            print(('wasb://niphd{host}@nipspark.blob.core.windows.net'
                  '/dailyCtr/{log_name}-{log_date}/00.done').format(
                host=host,
                log_name=log_name,
                log_date=log_date
            ))


if __name__ == '__main__':
    host = sys.argv[1]
    log_name = sys.argv[2]
    log_date = sys.argv[3]
    log_update_freq = sys.argv[4]
    monitor(host, log_name, log_date, log_update_freq)
