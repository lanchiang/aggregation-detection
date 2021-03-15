# Created by lan at 2021/3/10
import decimal
import json
import logging
import os
import time
from decimal import Decimal, InvalidOperation

import luigi
import numpy as np
from luigi.mock import MockTarget
from tqdm import tqdm

from data import detect_number_format, normalize_file
from dataprep import DataPreparation


class NumberFormatNormalization(luigi.Task):
    """
    This task runs the number format normalization task on the initial input file, and produces a set of valid number formats on each file.
    """

    dataset_path = luigi.Parameter()
    result_path = luigi.Parameter('/debug')
    sample_ratio = luigi.FloatParameter(default=0.2)
    debug = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)

    def output(self):
        if self.debug:
            return luigi.LocalTarget(os.path.join(self.result_path, 'normalize_number_format.jl'))
        else:
            return MockTarget(fn='number-format-normalization')

    def requires(self):
        return DataPreparation(self.dataset_path, self.result_path, debug=self.debug)

    def run(self):
        with self.input().open('r') as input_file:
            files_dict = [json.loads(line) for line in input_file]

            for file_dict in tqdm(files_dict, desc='Number format selection'):
                start_time = time.time()
                number_format = detect_number_format(np.array(file_dict['table_array']), self.sample_ratio)
                transformed_values_by_number_format = {}
                for nf in number_format:
                    transformed_values_by_number_format[nf] = normalize_file(file_dict['table_array'], nf)
                file_dict['valid_number_formats'] = transformed_values_by_number_format

                end_time = time.time()
                exec_time = end_time - start_time
                file_dict['exec_time'][self.__class__.__name__] = exec_time

        with self.output().open('w') as file_writer:
            for file_dict in files_dict:
                file_writer.write(json.dumps(file_dict) + '\n')


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
        use_default = True

    if use_default:
        try:
            value = Decimal(default)  # if a value cannot be converted to number, set it to zero.
            # Todo: setting the non-convertible value to zero does not make sense to all aggregation type, for example, average.
        except InvalidOperation as _:
            logging.getLogger('String to decimal').error('Given default value cannot be converted to a decimal.')
            exit(1)
    return value
