# coding: utf-8
from config import SCHEMA_PREFER


def read_schema(file_suffix):
    # file_name = FILENAME_PREFER + '_' + file_suffix
    file_name = eval(SCHEMA_PREFER + 'INPUT_' + file_suffix)
    schema_list = []
    with open(file_name, 'r') as openf:
        for line in openf.readlines():
            line = line.split('#')[0].strip()
            if len(line) != 0:
                schema_list.append(line)
    return schema_list


def read_key(file_suffix):
    return eval('KEY_' + file_suffix).split(',')


def read_output_schema():
    file_name = eval(SCHEMA_PREFER + 'OUTPUT')
    schema_list = []
    with open(file_name, 'r') as openf:
        for line in openf.readlines():
            line = line.split('#')[0].strip()
            if len(line) != 0:
                line = line.split(':')
                schema_list.append(line)
    return schema_list

if __name__ == "__main__":
    print(read_output_schema())
