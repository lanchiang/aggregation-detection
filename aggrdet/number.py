# Created by lan at 2021/3/10
import decimal
from decimal import Decimal

import numpy as np


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