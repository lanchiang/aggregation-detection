# Created by lan at 2021/1/2
import re
from typing import List

import numpy as np

from number import parse_number_string


class NumberFormat:

    def __init__(self, thousand_char: str, decimal_char: str, pattern: str) -> None:
        self.thousand_char = thousand_char
        self.decimal_char = decimal_char
        self.pattern = pattern


def normalize_file(file: np.ndarray, number_format: str):
    """
    Transform the number cells of the given file with the given number format.

    :param file: two dimensional file
    :param number_format: format of the numbers used for the given file.
    :return: a two-dimensional array transformed from the given file, where the numbers are formatted with the given number format,
    and a 2er-tuple that indicates the numeric line indices, first element for row indices, second element for column indices.
    Todo: Right now a line is numeric as long as it contains at least one numeric cell.
    """
    trans_file = np.copy(file)
    file_cell_data_types = np.full_like(file, fill_value='S', dtype='object')
    numeric_line_indices = ({}, {})
    # numeric_line_indices = ([], [])
    for index, value in np.ndenumerate(trans_file):
        processed_value = parse_number_string(value)
        matches = re.match(NumberFormatPattern.get(number_format), processed_value.strip())
        trans_file[index] = normalize_number(matches[0], number_format) if matches is not None else processed_value
        if matches is not None:
            if index[0] not in numeric_line_indices[0]:
                numeric_line_indices[0][index[0]] = []
            numeric_line_indices[0][index[0]].append(index[1])

            if index[1] not in numeric_line_indices[1]:
                numeric_line_indices[1][index[1]] = []
            numeric_line_indices[1][index[1]].append(index[0])
            # numeric_line_indices[0].append(index[0])
            # numeric_line_indices[1].append(index[1])
            file_cell_data_types[index] = 'N'
        if processed_value == '':
            file_cell_data_types[index] = 'E'
    # numeric_line_indices = (sorted(list(set(numeric_line_indices[0]))), list(set(numeric_line_indices[1])))
    return trans_file.tolist(), numeric_line_indices, file_cell_data_types


def normalize_number(value: str, number_format: str) -> str:
    """
    Transform a string that essentially represents a number to the corresponding number with the given number format.

    Return a string that includes the transformed number. If the given number format does not match any supported one, return the given string.

    :param value: the string
    :param number_format: number format with which the value is normalized
    :return: the normalized string
    """
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


def detect_number_format(file: np.ndarray, sample=0.2) -> str:
    """
    Try to detect the number format used in the given file.

    To speed up the detection process, a sample size can be specified to let the function determine a number format by checking the first a few numeric cells.

    :param file: the target file where the number format needs to be detected.
    :param sample: size of samples inspected to determine the number format. By default using up to 20% of all numeric cells. At least two such cells must be inspected.
    :return:
    """
    formats2values = {}
    if sample:
        test_set_size = int(file.size * sample)
        test_set_size = file.size if test_set_size < 2 else test_set_size
    else:
        test_set_size = file.size

    # Todo: the way applicable number formats are computed is wrong. How?
    count = 0
    for index, value in np.ndenumerate(file):
        if not value:
            continue
        fit_formats = sniff_number_format(value.strip())
        if fit_formats:
            for ff in fit_formats:
                if ff not in formats2values:
                    formats2values[ff] = []
                formats2values[ff].append(value)
            count += 1
        if count == test_set_size:
            break

    # Todo: now does not work due to numeric row/column headers.
    # Return only those that covers all the tested cells.
    # fit_formats = {k: v for k, v in formats2values.items() if len(v) == count}
    # formats2values = fit_formats if len(fit_formats) > 0 else formats2values
    # return detected
    return formats2values.keys()


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

    :param value: value string
    :return: all applicable number formats on the given value string
    """
    parsed_number_string = parse_number_string(value)
    fit_formats = []
    for format_pattern in NumberFormatPattern.items():
        pattern = format_pattern[1]
        matches = re.match(pattern, parsed_number_string)
        if matches is not None:
            fit_formats.append(format_pattern[0])
    return fit_formats
