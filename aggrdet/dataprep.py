# Created by lan at 2021/2/11
import gzip
import json
import os
import re
import shutil
from decimal import Decimal

import luigi
import numpy as np
from tqdm import tqdm

from data import normalize_number_value


class LoadDataset(luigi.Task):
    """
    This task loads the dataset stored in a json.jl.gz compressed file into the memory.
    """
    dataset_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('temp/file.jl')

    def run(self):
        with gzip.open(self.dataset_path, mode='r') as ds_json_file:
            json_file_dicts = np.array([json.loads(line) for line in ds_json_file])
            dataset = [json.dumps({'file_name': jfd['file_name'],
                                   'table_id': jfd['table_id'],
                                   'table_array': jfd['table_array'],
                                   'aggregation_annotations': jfd['aggregation_annotations'],
                                   'number_format': jfd['number_format'],
                                   'annotations': jfd['annotations']}) for jfd in tqdm(json_file_dicts)]

        with self.output().open('w') as file_writer:
            for curated_json_file in dataset:
                file_writer.write(curated_json_file + '\n')


class DataPreparation(luigi.Task):

    dataset_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('temp/file-error-level-adjusted.jl')

    def requires(self):
        return LoadDataset(self.dataset_path)

    def run(self):
        with self.input().open('r') as file_reader:
            json_file_dicts = np.array([json.loads(line) for line in file_reader])

        for file_dict in tqdm(json_file_dicts):
            # print(file_dict['file_name'])
            # if file_dict['file_name'] != 'C10003':
            #     continue
            file_values = np.copy(file_dict['table_array'])
            for index, value in np.ndenumerate(file_values):
                normalized_value = normalize_number_value(value, file_dict['number_format'])
                file_values[index] = normalized_value
            if not file_dict['aggregation_annotations']:
                continue
            for aggr_annotation in file_dict['aggregation_annotations']:
                aggor_index = tuple(aggr_annotation['aggregator_index'])
                # if aggor_index != (6,5):
                #     continue
                aggor_value = re.sub('[^0-9,.\-+\s]', '', file_values[aggor_index])
                if bool(aggor_value):
                    try:
                        aggor_value = Decimal(aggor_value)
                    except Exception:
                        aggor_value = Decimal(0.0)
                else:
                    aggor_value = Decimal(0.0)
                aggee_indices = [tuple(aggee_index) for aggee_index in aggr_annotation['aggregatee_indices']]
                _aggee_values = [re.sub('[^0-9,.\-+\s]', '', file_values[aggee_index]) for aggee_index in aggee_indices]
                aggee_values = []
                for aggee_value in _aggee_values:
                    if bool(aggee_value):
                        try:
                            aggee_value = Decimal(aggee_value)
                        except Exception:
                            aggee_value = Decimal(0.0)
                    else:
                        aggee_value = Decimal(0.0)
                    aggee_values.append(aggee_value)
                operator = aggr_annotation['operator']
                if operator == 'Sum':
                    expected = sum([aggee_value for aggee_value in aggee_values])
                    actual = aggor_value
                elif operator == 'Subtract':
                    expected1 = abs(aggee_values[0] - aggee_values[1])
                    expected2 = abs(aggee_values[1] - aggee_values[0])
                    actual = abs(aggor_value)
                    expected = expected1 if abs(actual - expected1) < abs(actual - expected2) else expected2
                elif operator == 'Average':
                    expected = sum([aggee_value for aggee_value in aggee_values]) / len(aggee_values)
                    actual = aggor_value
                elif operator == 'Percentage':
                    # Todo:
                    actual = aggor_value
                    expected = actual + Decimal(aggr_annotation['error_bound'])
                    pass
                else:
                    raise RuntimeError('Should not come here.')
                error = abs(expected - actual)
                if float(error) == 182.6:
                    stop = 0
                aggr_annotation['error_bound'] = float(error)
            pass

        with self.output().open('w') as file_writer:
            for file_dict in json_file_dicts:
                file_writer.write(json.dumps(file_dict) + '\n')


if __name__ == '__main__':
    luigi.run()