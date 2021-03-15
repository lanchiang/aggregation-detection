# Created by lan at 2021/2/11
import gzip
import json
import os
import re
from decimal import Decimal

import luigi
import numpy as np
from luigi.mock import MockTarget
from tqdm import tqdm

from data import normalize_file
from helpers import AggregationOperator
from number import str2decimal


class LoadDataset(luigi.Task):
    """
    This task loads a dataset stored in a json.jl.gz compressed file into the memory, selecting only those entries that are useful to aggregation detection.
    """

    dataset_path = luigi.Parameter()
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
                                   'table_array': jfd['table_array'],
                                   'aggregation_annotations': jfd['aggregation_annotations'],
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
    debug = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)

    result_path = luigi.Parameter('/debug/')

    def output(self):
        if self.debug:
            return luigi.LocalTarget(os.path.join(self.result_path, 'data-preparation.jl'))
        else:
            return MockTarget('data-preparation')

    def requires(self):
        return LoadDataset(dataset_path=self.dataset_path, debug=self.debug, result_path=self.result_path)

    def run(self):
        with self.input().open('r') as file_reader:
            file_dicts = np.array([json.loads(line) for line in file_reader])

        for file_dict in tqdm(file_dicts, desc='Data preparation'):
            normalized_file_content = np.array(normalize_file(np.copy(file_dict['table_array']), file_dict['number_format']))
            for annotation in file_dict['aggregation_annotations']:
                aggor_index = tuple(annotation['aggregator_index'])
                aggor_value = re.sub('[^0-9,.\-+\s]', '', normalized_file_content[aggor_index])  # remove all characters that cannot appear in a numeric value
                aggee_indices = [tuple(aggee_index) for aggee_index in annotation['aggregatee_indices']]
                _aggee_values = [re.sub('[^0-9,.\-+\s]', '', normalized_file_content[aggee_index]) for aggee_index in aggee_indices]
                operator = annotation['operator']
                if operator == AggregationOperator.SUM.value:
                    aggor_value = str2decimal(aggor_value, 0)
                    aggee_values = [str2decimal(aggee_value, 0) for aggee_value in _aggee_values]
                    expected = sum([aggee_value for aggee_value in aggee_values])
                    actual = aggor_value
                elif operator == AggregationOperator.SUBTRACT.value:
                    aggor_value = str2decimal(aggor_value, 0)
                    aggee_values = [str2decimal(aggee_value, 0) for aggee_value in _aggee_values]
                    expected = aggee_values[0] - aggee_values[1]
                    actual = aggor_value
                    pass
                elif operator == AggregationOperator.AVERAGE.value:
                    expected = sum([aggee_value for aggee_value in aggee_values]) / len(aggee_values)
                    actual = aggor_value
                    pass
                elif operator == AggregationOperator.PERCENTAGE.value:
                    # Todo: how to?:
                    actual = aggor_value
                    expected = actual + Decimal(annotation['error_bound'])
                    pass
                else:
                    raise RuntimeError("Given aggregation operator string is illegal.")

                # error = abs(expected - actual)
                error = expected - actual
                if actual == 0.0:
                    true_error_level = -1.0
                else:
                    true_error_level = abs((expected - actual) / actual)
                # absolute error level
                # Todo: can I safely remove the absolute error level?
                annotation['error_bound'] = float(error)

                # relative error level
                annotation['error_level_percent'] = float(true_error_level)
                pass
            pass

        with self.output().open('w') as file_writer:
            for file_dict in file_dicts:
                file_writer.write(json.dumps(file_dict) + '\n')


if __name__ == '__main__':
    luigi.run()