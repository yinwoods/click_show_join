import sys
import subprocess
from config import logs_config_table

def pre_process(log, dest_pos):
    if log.update_freq == 'hourly' and log.dir_level == 2:
        with open(dest_pos, 'r') as f:
            lines = list(f.readlines())

        new_file = []
        file_size = 0
        for line in lines:
            line = filter(lambda x: len(x) > 0, line.strip().split(' '))
            child_size, pos = list(map(lambda x: x.strip(), line))
            if not pos.strip().endswith('done'):
                file_size += int(child_size)
            else:
                new_file.append((file_size, pos))
                file_size = 0

        for line in new_file:
            pos = str(line[1].strip())
            pos = pos.replace('.done', '')
            print(line[0], pos, sep='\t')
    elif log.update_freq == 'hourly' and log.dir_level == 1:
        with open(dest_pos, 'r') as f:
            lines = list(f.readlines())

        new_file = []
        for line in lines:
            line = filter(lambda x: len(x) > 0, line.strip().split(' '))
            file_size, pos = list(map(lambda x: x.strip(), line))
            if not pos.strip().endswith('done'):
                new_file.append((file_size, pos))

        for line in new_file:
            pos = str(line[1].strip())
            pos = pos.replace('.done', '')
            print(line[0], pos, sep='\t')

def monitor(host, name, date):
    for log in logs_config_table:
        if log.name == name:
            if log.update_freq == 'hourly':
                url = ('wasb://niphd{host}@nipspark.blob.core.windows.net/dailyCtr/{name}-{date}/*').format(
                        host=host,
                        name=log.name,
                        date=date
                    )
                command = 'hdfs dfs -du {} > tmp'.format(url)
                subprocess.call(command, shell=True)
                pre_process(log, 'tmp')
            elif log.update_freq == 'daily':
                url = ('wasb://niphd{host}@nipspark.blob.core.windows.net' '/dailyCtr/{name}-{date}').format(
                        host=host,
                        name=log.name,
                        date=date
                    )
                command = 'hdfs dfs -du {} > tmp'.format(url)
                state = subprocess.call(command, shell=True)
                if state == 0:
                    res = []
                    with open('tmp', 'r') as f:
                        for line in f:
                            res.append('\t'.join(filter(lambda x: len(x) > 0, line.split(' '))))

                    with open('tmp', 'w') as f:
                        for line in res:
                            print(line)


if __name__ == '__main__':
    host = sys.argv[1]
    name = sys.argv[2]
    date = sys.argv[3]
    monitor(host, name, date)
