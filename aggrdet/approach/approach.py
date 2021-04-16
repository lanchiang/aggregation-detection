# Created by lan at 2021/3/14
import os
from abc import ABC, abstractmethod
from copy import copy

import luigi
from concurrent.futures import TimeoutError

from dataprep import NumberFormatNormalization
from tree import AggregationRelationForest


class Approach(ABC):

    @abstractmethod
    def detect_row_wise_aggregations(self, file_dict):
        pass

    @abstractmethod
    def detect_column_wise_aggregations(self, file_dict):
        pass

    @abstractmethod
    def detect_proximity_aggregation_relations(self, forest: AggregationRelationForest, error_bound: float, error_strategy):
        pass


class AggregationDetection(luigi.Task, Approach):
    dataset_path = luigi.Parameter()
    result_path = luigi.Parameter(default='/debug/')
    error_level = luigi.FloatParameter(default=0)
    use_extend_strategy = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    use_delayed_bruteforce = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    timeout = luigi.FloatParameter(default=300)
    debug = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)

    NUMERIC_SATISFIED_RATIO = 0.5
    operator = ''

    cpu_count = int(os.cpu_count() * 0.5)

    def requires(self):
        return NumberFormatNormalization(self.dataset_path, self.result_path,
                                         self.error_level, self.use_extend_strategy, self.use_delayed_bruteforce, self.timeout,
                                         debug=self.debug)

    def detect_aggregations(self, file_dict):
        row_wise_aggregations = self.detect_row_wise_aggregations(file_dict)
        column_wise_aggregations = self.detect_column_wise_aggregations(file_dict)
        return row_wise_aggregations, column_wise_aggregations

    def process_results(self, results, files_dict_map, task_name):
        while True:
            try:
                result = next(results)
            except StopIteration:
                break
            except TimeoutError:
                pass
            except ValueError:
                continue
            else:
                row_wise_result = result[0]
                column_wise_result = result[1]
                file_name = row_wise_result['file_name']
                sheet_name = row_wise_result['table_id']
                merged_result = copy(row_wise_result)
                for nf in merged_result['aggregation_detection_result'].keys():
                    merged_result['aggregation_detection_result'][nf].extend(column_wise_result['aggregation_detection_result'][nf])
                merged_result['exec_time'][task_name] = \
                    merged_result['exec_time']['RowWiseDetection'] + merged_result['exec_time']['ColumnWiseDetection']
                files_dict_map[(file_name, sheet_name)] = merged_result


class BruteForce(Approach):
    pass
