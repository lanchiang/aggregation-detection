# Created by lan at 2021/1/2
import re
from typing import List

import numpy as np

from reader import load_dataset, get_file


class NumberFormat:

    def __init__(self, thousand_char: str, decimal_char: str, pattern: str) -> None:
        self.thousand_char = thousand_char
        self.decimal_char = decimal_char
        self.pattern = pattern


def transform_number_format(table: np.ndarray, number_format: str) -> np.ndarray:
    curated_table = np.copy(table)
    for index, value in np.ndenumerate(curated_table):
        matches = re.match(NumberFormatPattern.get(number_format), value.strip())
        curated_table[index] = normalize_number_value(matches[0], number_format) if matches is not None else value
    return curated_table.tolist()


def normalize_number_value(value: str, number_format: str) -> str:
    if number_format == 'COMMA_POINT' or number_format == 'Comma Point':
        nor_str = re.sub(pattern=',', repl='', string=value)
    elif number_format == 'POINT_COMMA' or number_format == 'Point Comma':
        nor_str = re.sub(pattern=',', repl='.', string=re.sub(pattern='\.', repl='', string=value))
    elif number_format == 'SPACE_POINT' or number_format == 'Space Point':
        nor_str = re.sub(pattern='\s', repl='', string=value)
    elif number_format == 'SPACE_COMMA' or number_format == 'Space Comma':
        nor_str = re.sub(pattern=',', repl='.', string=re.sub(pattern='\s', repl='', string=value))
    elif number_format == 'NONE_COMMA' or number_format == 'None Comma':
        nor_str = re.sub(pattern=',', repl='.', string=value)
    else:
        nor_str = value
    return nor_str


def detect_number_format(table: np.ndarray, sample=0.5) -> str:
    """
    Detect the number format used in the given table.
    :param table: the target table of which the number format needs to be detected.
    :param sample:
    :return:
    """
    test_set_fit_formats = {}
    if sample:
        test_set_size = int(table.size * sample)
        test_set_size = table.size if test_set_size < 2 else test_set_size
    else:
        test_set_size = table.size

    # Todo: the way applicable number formats are computed is wrong.
    count = 0
    for index, value in np.ndenumerate(table):
        if not value:
            continue
        fit_formats = sniff_number_format(value.strip())
        if fit_formats:
            for ff in fit_formats:
                if ff not in test_set_fit_formats:
                    test_set_fit_formats[ff] = []
                test_set_fit_formats[ff].append(value)
            count += 1
        if count == test_set_size:
            break

    # Return only those that covers all the tested cells.
    # Todo: now does not work due to numberic row/column headers.
    # fit_formats = {k: v for k, v in test_set_fit_formats.items() if len(v) == count}
    # test_set_fit_formats = fit_formats if len(fit_formats) > 0 else test_set_fit_formats
    # return detected
    return test_set_fit_formats.keys()


NumberFormatPattern = {
    'Comma Point': '^([+-])?(\d{1,3})(\,\d{3})*(\.\d{1,})?$',
    'Point Comma': '^([+-])?(\d{1,3})(\.\d{3})*(\,\d{1,})?$',
    'Space Point': '^([+-])?(\d{1,3})(\s\d{3})*(\.\d{1,})?$',
    'Space Comma': '^([+-])?(\d{1,3})(\s\d{3})*(\,\d{1,})?$',
    'None Point': '^[+-]?(\d+)(\.\d{1,})?$',
    'None Comma': '^[+-]?(\d+)(\,\d{1,})?$',
}


def sniff_number_format(value: str) -> List[str]:
    """
    Sniff all number formats that are applicable for this string.
    :param value:
    :return:
    """
    fit_formats = []
    for format_pattern in NumberFormatPattern.items():
        pattern = format_pattern[1]
        matches = re.match(pattern, value)
        if matches is not None:
            fit_formats.append(format_pattern[0])
    return fit_formats


if __name__ == '__main__':
    path = '../data/troy.jl.gz'
    dataset = load_dataset(path)
    file = get_file(dataset, 'C10159', 'C10159')

    # number_format = detect_number_format(file[2])
    # print(number_format)