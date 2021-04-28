# Created by lan at 2021/4/28
import itertools
import os
from copy import copy, deepcopy

import luigi
import pandas
from luigi.mock import MockTarget

from approach.CollectiveAggregationDetectionTask import CollectiveAggregationDetectionTask
from approach.aggrdet.supplemental.SupplementalAggregationDetection import SupplementalAggregationDetection
from data import normalize_file
from helpers import AggregationOperator
from tree import AggregationRelationForest


class SupplementalSumDetection(SupplementalAggregationDetection):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.operator = AggregationOperator.SUM.value
        self.task_name = self.__class__.__name__
        self.error_level = self.error_level_dict[self.operator] if self.operator in self.error_level_dict else self.error_level

    def output(self):
        if self.debug:
            return luigi.LocalTarget(os.path.join(self.result_path, 'supplemental-sum-detection.jl'))
        else:
            return MockTarget('supplemental-sum-detection')

    def requires(self):
        return CollectiveAggregationDetectionTask(dataset_path=self.dataset_path,
                                                  result_path=self.result_path,
                                                  error_level_dict=self.error_level_dict,
                                                  target_aggregation_type=self.target_aggregation_type,
                                                  use_extend_strategy=self.use_extend_strategy,
                                                  use_delayed_bruteforce=self.use_delayed_bruteforce,
                                                  timeout=self.timeout,
                                                  debug=self.debug)

    def ignore_existing(self, file_dict, signatures, axis):
        file_dicts = self.construct_new_file(file_dict, signatures, axis)
        pass

    def combine_existing(self, file_dict, signatures, axis):
        pass

    def construct_new_file(self, file_dict, signatures, axis):
        file_dicts = []
        if axis == 0:
            ignored_column_indices = []
            ignored_column_indices.extend([signature[0] for signature in signatures
                                           if signature[2] == AggregationOperator.AVERAGE.value or signature[2] == AggregationOperator.RELATIVE_CHANGE.value])
            division_ignored_column_indices = [[signature[0], signature[1][1]] for signature in signatures if signature[2] == AggregationOperator.DIVISION.value]
            division_solutions = itertools.product(*division_ignored_column_indices)
            ignored_column_indices_solutions = [ignored_column_indices + list(division_solution) for division_solution in division_solutions]
            for ignored_column_indices_solution in ignored_column_indices_solutions:
                _file_dict = deepcopy(file_dict)
                df = pandas.DataFrame(_file_dict['table_array'])
                _file_dict['table_array'] = df.drop(df.columns[ignored_column_indices_solution], axis=1).values.tolist()
                _file_dict['num_cols'] = _file_dict['num_cols'] - len(ignored_column_indices_solution)

                reduced_column_indices = [index for index in range(df.shape[1]) if index not in ignored_column_indices_solution]
                _file_dict['index_mapping'] = reduced_column_indices

                numeric_line_indices = {}
                for number_format, formatted_values in _file_dict['valid_number_formats'].items():
                    df = pandas.DataFrame(formatted_values)
                    _file_dict['valid_number_formats'][number_format] = df.drop(df.columns[ignored_column_indices_solution], axis=1).values.tolist()
                    _, numeric_line_indices[number_format], _ = normalize_file(_file_dict['table_array'], number_format)
                _file_dict['numeric_line_indices'] = numeric_line_indices
                file_dicts.append(_file_dict)
            pass
        elif axis == 1:
            pass
        else:
            raise RuntimeError('Given axis parameter %s is illegal' % axis)
        return file_dicts