#!/usr/bin/env python

from itertools import groupby
from operator import itemgetter
import sys
from util import read_output_schema


def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator)


def main(separator='\t'):
    data = read_mapper_output(sys.stdin, separator=separator)
    output_schema = read_output_schema()

    for key1, group in groupby(data, itemgetter(0, 1)):
        '''show_or_click_data:
            {key:
                {1(linetype):
                    {pageid:*...}
                    ...
                }
            ...
            }'''
        show_or_click_data = dict()

        for line in group:
            userid, newsid, pid, pindex, line_type = line[:5]
            if line_type == '3':
                key = 'zan'
            else:
                key = '.'.join(line[:4])
            show_or_click_data.setdefault(key, dict())
            show_or_click_data[key].setdefault(
                line_type, dict(
                    zip(eval('input_%s_schema' % line_type), line[5:])))

        for key, values in show_or_click_data.items():
            if key == 'zan':
                continue
            if values.get('1', None) is not None:
                if (values.get('2', None) is not None
                   and show_or_click_data.get('zan', None) is not None):
                    values['3'] = show_or_click_data['zan']['3']
                    del show_or_click_data['zan']
                output_data = []
                for (schema, suffix, default) in output_schema:
                    if suffix != '0':
                        output_data.append(
                            values.get(suffix, dict()).get(schema, default))
                    else:
                        output_data.append(
                            '0' if values.get('2', None) is None else '1')
                print('\t'.join(output_data))

if __name__ == "__main__":
    main()
