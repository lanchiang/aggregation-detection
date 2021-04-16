# Created by lan at 2021/2/11
import decimal
import gzip
import json
import math
import os
import re
import time
from decimal import Decimal

import luigi
import numpy as np
from luigi.mock import MockTarget
from tqdm import tqdm

from data import normalize_file, detect_number_format
from helpers import AggregationOperator
from number import str2decimal


class LoadDataset(luigi.Task):
    """
    This task loads a dataset stored in a json.jl.gz compressed file into the memory, selecting only those entries that are useful to aggregation detection.
    """

    dataset_path = luigi.Parameter()
    error_level = luigi.FloatParameter(default=0)
    use_extend_strategy = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    use_delayed_bruteforce = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    timeout = luigi.FloatParameter(default=300)
    debug = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    result_path = luigi.Parameter('/debug/')

    def output(self):
        if self.debug:
            return luigi.LocalTarget(os.path.join(self.result_path, 'load-dataset.jl'))
        else:
            return MockTarget('dataset-loading')

    def run(self):
        with gzip.open(self.dataset_path, mode='r') as ds_json_file:
            json_file_dicts = np.array([json.loads(line) for line in ds_json_file])
            dataset = [json.dumps({'file_name': jfd['file_name'],
                                   'table_id': jfd['table_id'],
                                   'num_rows': jfd['num_rows'],
                                   'num_cols': jfd['num_cols'],
                                   'table_array': jfd['table_array'],
                                   'aggregation_annotations': jfd['aggregation_annotations'],
                                   'parameters': {'error_level': self.error_level,
                                                  'use_extend_strategy': self.use_extend_strategy,
                                                  'use_delayed_bruteforce_strategy': self.use_delayed_bruteforce,
                                                  'timeout': self.timeout},
                                   'number_format': jfd['number_format'],
                                   'exec_time': {}}) for jfd in json_file_dicts]

        with self.output().open('w') as file_writer:
            for curated_json_file in dataset:
                file_writer.write(curated_json_file + '\n')


class DataPreparation(luigi.Task):
    """
    This task prepare dataset, calculate the real error level for each aggregation in the ground truth.
    """

    dataset_path = luigi.Parameter()
    result_path = luigi.Parameter('/debug/')
    error_level = luigi.FloatParameter(default=0)
    use_extend_strategy = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    use_delayed_bruteforce = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    timeout = luigi.FloatParameter(default=300)
    debug = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)

    def output(self):
        if self.debug:
            return luigi.LocalTarget(os.path.join(self.result_path, 'data-preparation.jl'))
        else:
            return MockTarget('data-preparation')

    def requires(self):
        return LoadDataset(error_level=self.error_level, use_extend_strategy=self.use_extend_strategy,
                           use_delayed_bruteforce=self.use_delayed_bruteforce, timeout=self.timeout,
                           dataset_path=self.dataset_path, debug=self.debug, result_path=self.result_path)

    def run(self):
        with self.input().open('r') as file_reader:
            file_dicts = np.array([json.loads(line) for line in file_reader])

        for file_dict in tqdm(file_dicts, desc='Data preparation'):
            normalized_file_arr, _ = normalize_file(np.copy(file_dict['table_array']), file_dict['number_format'])
            normalized_file_content = np.array(normalized_file_arr)
            cleaned_aggregation_annotations = []
            for annotation in file_dict['aggregation_annotations']:
                aggor_index = tuple(annotation['aggregator_index'])
                # if aggor_index != (77, 8):
                #     continue

                # Todo: has problem: what if the number is negative?
                aggor_value = re.sub('[^0-9,.\-+\s]', '', normalized_file_content[aggor_index])  # remove all characters that cannot appear in a numeric value
                aggee_indices = [tuple(aggee_index) for aggee_index in annotation['aggregatee_indices']]
                _aggee_values = [re.sub('[^0-9,.\-+\s]', '', normalized_file_content[aggee_index]) for aggee_index in aggee_indices]
                operator = annotation['operator']
                if operator == AggregationOperator.SUM.value:
                    aggor_value = str2decimal(aggor_value, 0)
                    aggee_values = [str2decimal(aggee_value, 0) for aggee_value in _aggee_values]
                    expected = sum([aggee_value for aggee_value in aggee_values])
                    actual = aggor_value
                    error = expected - actual
                elif operator == AggregationOperator.SUBTRACT.value:
                    aggor_value = str2decimal(aggor_value, 0)
                    aggee_values = [str2decimal(aggee_value, 0) for aggee_value in _aggee_values]
                    expected = aggee_values[0] - aggee_values[1]
                    actual = aggor_value
                    error = expected - actual
                elif operator == AggregationOperator.AVERAGE.value:
                    aggor_value = str2decimal(aggor_value, 0)
                    aggee_values = [str2decimal(aggee_value, 0) for aggee_value in _aggee_values]
                    expected = sum([aggee_value for aggee_value in aggee_values]) / len(aggee_values)
                    actual = aggor_value
                    error = expected - actual
                elif operator == AggregationOperator.PERCENTAGE.value:
                    aggor_value = str2decimal(aggor_value, None)
                    aggee_values = [str2decimal(aggee_value, None) for aggee_value in _aggee_values]
                    if aggor_value is None or any([aggee_value is None for aggee_value in aggee_values]):
                        actual = 0
                        expected = math.inf
                    else:
                        actual = aggor_value
                        try:
                            percentage = aggee_values[0] / aggee_values[1]
                        except decimal.DivisionByZero:
                            percentage = math.inf
                        except decimal.InvalidOperation:
                            continue
                        try:
                            relative_increment = (aggee_values[1] - aggee_values[0]) / aggee_values[0]
                        except decimal.DivisionByZero:
                            relative_increment = math.inf
                        except decimal.InvalidOperation:
                            continue
                        expected = min(percentage, relative_increment)
                    error = min(expected - actual, expected, actual * 100)
                else:
                    raise RuntimeError("Given aggregation operator string is illegal.")

                # error = expected - actual
                if actual == 0.0:
                    true_error_level = -1.0
                else:
                    true_error_level = abs((expected - actual) / actual)
                if true_error_level >= 1:
                    continue
                # absolute error level
                # Todo: can I safely remove the absolute error level?
                annotation['error_bound'] = float(error)

                # relative error level
                annotation['error_level_percent'] = float(true_error_level)
                cleaned_aggregation_annotations.append(annotation)
                pass
            file_dict['aggregation_annotations'] = cleaned_aggregation_annotations
            pass

        with self.output().open('w') as file_writer:
            for file_dict in file_dicts:
                file_writer.write(json.dumps(file_dict) + '\n')


class NumberFormatNormalization(luigi.Task):
    """
    This task runs the number format normalization task on the initial input file, and produces a set of valid number formats on each file.
    """

    dataset_path = luigi.Parameter()
    result_path = luigi.Parameter('/debug')
    error_level = luigi.FloatParameter(default=0)
    use_extend_strategy = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    use_delayed_bruteforce = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    timeout = luigi.FloatParameter(default=300)
    sample_ratio = luigi.FloatParameter(default=0.2)

    debug = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)

    def output(self):
        if self.debug:
            return luigi.LocalTarget(os.path.join(self.result_path, 'normalize_number_format.jl'))
        else:
            return MockTarget(fn='number-format-normalization')

    def requires(self):
        return DataPreparation(self.dataset_path, self.result_path,
                               self.error_level, self.use_extend_strategy, self.use_delayed_bruteforce, self.timeout,
                               debug=self.debug)

    def run(self):
        with self.input().open('r') as input_file:
            files_dict = [json.loads(line) for line in input_file]

            for file_dict in tqdm(files_dict, desc='Number format selection'):
                start_time = time.time()
                number_format = detect_number_format(np.array(file_dict['table_array']), self.sample_ratio)
                transformed_values_by_number_format = {}
                numeric_line_indices = {}
                for nf in number_format:
                    transformed_values_by_number_format[nf], numeric_line_indices[nf] = normalize_file(file_dict['table_array'], nf)
                file_dict['valid_number_formats'] = transformed_values_by_number_format
                file_dict['numeric_line_indices'] = numeric_line_indices

                end_time = time.time()
                exec_time = end_time - start_time
                file_dict['exec_time'][self.__class__.__name__] = exec_time

        with self.output().open('w') as file_writer:
            for file_dict in files_dict:
                file_writer.write(json.dumps(file_dict) + '\n')


if __name__ == '__main__':
    luigi.run()