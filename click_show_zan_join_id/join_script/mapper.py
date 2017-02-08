#!/usr/bin/env python
import sys
import os
from util import read_key


# 'file' in this case is STDIN
def read_input(file, separator):
    # Split each line into words
    for line in file:
        yield line.strip().split(separator)


def get_file_suffix(file_path):
    if 'NewsImpressionSummary' in file_path:
        return '1'
    elif 'NewsClickSummary' in file_path:
        return '2'
    elif 'dashboard_offline_dt' in file_path:
        return '3'
    else:
        return None


def main(separator='\t'):
    # Read the data using read_input
    data = read_input(sys.stdin, separator)
    # Process each words returned from read_input
    for line in data:
        if len(line[0]) < 4:
            continue
        file_suffix = get_file_suffix(os.environ['map_input_file'])
        if file_suffix is not None:
            # schema = read_schema(file_suffix)
            key_list = read_key(file_suffix)
            map_out = []
            for key_index in key_list:
                if key_index != '-':
                    map_out.append(line[int(key_index)])
                else:
                    map_out.append('-')
            map_out.append(file_suffix)
            map_out.extend(line)
            print('\t'.join(map_out))


if __name__ == "__main__":
    main()
