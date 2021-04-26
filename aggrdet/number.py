# Created by lan at 2021/3/10
import decimal
import logging
import re
from decimal import Decimal, InvalidOperation

import numpy as np

currency_symbols = ['$', '€', '£']


def get_indices_number_cells(file_array) -> set:
    numberized_indices = set()
    for index, value in np.ndenumerate(file_array):
        try:
            Decimal(value)
        except decimal.InvalidOperation:
            continue
        else:
            numberized_indices.add(index)
    return numberized_indices


def str2decimal(value, default=0.0):
    use_default = False
    if bool(value):
        try:
            value = Decimal(value)
        except InvalidOperation as _:
            use_default = True
        else:
            if value.is_nan():
                use_default = True
    else:
        use_default = True

    if use_default:
        if default is None:
            value = default
        else:
            try:
                value = Decimal(default)  # if a value cannot be converted to number, set it to zero.
            except InvalidOperation as _:
                logging.getLogger('String to decimal').error('Given default value cannot be converted to a decimal.')
                raise RuntimeError('Given default value cannot be converted to a decimal.')
    return value


def parse_number_string(value: str) -> str:
    """
    Parse the number string with heuristics.
    For example, if the number is wrapped with a pair of round brackets, remove the brackets and pre-fix the number with a "-"

    :param value:
    :return:
    """
    wrapped_with_brackets_pattern = '^\(.+\)$'
    matches = re.match(wrapped_with_brackets_pattern, value)
    if matches:
        processed_value = '-' + value[1:len(value)-1]
    else:
        processed_value = value

    if processed_value.endswith('%'):
        processed_value = processed_value.rstrip('%')
        try:
            processed_value = Decimal(processed_value)
        except decimal.InvalidOperation as ioe:
            pass
        else:
            processed_value = str(Decimal(processed_value / 100))

    # all currency characters are removed
    processed_value = ''.join(i for i in processed_value if not i in currency_symbols)
    # processed_value = re.sub('[^0-9,.\-+\s]', '', processed_value)
    processed_value = processed_value.strip()
    return processed_value
