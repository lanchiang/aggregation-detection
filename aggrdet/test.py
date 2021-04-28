# Created by lan at 2021/3/14
import json
from dataclasses import dataclass

import pandas


@dataclass(order=True)
class TestCellIndex:
    row_index: int
    column_index: int


if __name__ == '__main__':
    data_path = '/Users/lan/Desktop/experiments.csv'
    output_path = '/Users/lan/Desktop/ee.csv'

    with open(data_path, mode='rb') as file_reader:
        dicts = [json.loads(line) for line in file_reader]

    # header = ['sum_precision', 'average_precision', 'division_precision', 'relative_change_precision']
    # header = ['sum_recall', 'average_recall', 'division_recall', 'relative_change_recall']
    header = ['sum_f1', 'average_f1', 'division_f1', 'relative_change_f1']

    table = [[dict['Sum'], dict['Average'], dict['Division'], dict['RelativeChange']] for dict in dicts]

    df = pandas.DataFrame(table, columns=header, index=None)

    df.to_csv(output_path)

    pass
