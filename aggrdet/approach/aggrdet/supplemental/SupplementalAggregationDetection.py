# Created by lan at 2021/4/28
import ast
import math
import time
from abc import ABC, abstractmethod
from copy import deepcopy

import numpy as np

from approach.approach import AggregationDetection
from elements import CellIndex, Cell
from helpers import AggregationDirection
from tree import AggregationRelationForest


class SupplementalAggregationDetection(AggregationDetection, ABC):

    @abstractmethod
    def ignore_existing(self, file_dict, signatures, axis):
        """
        Detecting aggregations on the file where the detected ones are omitted.
        :return:
        """
        pass

    @abstractmethod
    def combine_existing(self, file_dict, signatures, axis):
        """
        Detecting aggregations on the combination of the existing ones.
        :return:
        """
        pass

    def detect_row_wise_aggregations(self, file_dict):
        start_time = time.time()
        _file_dict = deepcopy(file_dict)

        for number_format in _file_dict['valid_number_formats']:
            if number_format != _file_dict['number_format']:
                continue

            aggregation_line_signature = self.__get_aggregation_line_signature(_file_dict['aggregation_detection_result'][number_format], 0)
            self.ignore_existing(file_dict=_file_dict, signatures=aggregation_line_signature, axis=0)
        pass

    def detect_column_wise_aggregations(self, file_dict):
        pass

    def __get_aggregation_line_signature(self, aggrdet_results, axis=0):
        """

        :param aggrdet_results:
        :param axis:
        :return:
        """
        aggregation_line_signatures = []
        if axis == 0:
            # row wise
            row_wise_results = [result for result in aggrdet_results if result[3] == AggregationDirection.ROW_WISE.value]
            for result in row_wise_results:
                aggregator_column_index = ast.literal_eval(result[0])[1]
                aggregatee_column_indices = tuple([ast.literal_eval(aggregatee_index)[1] for aggregatee_index in result[1]])
                aggregation_line_signatures.append((aggregator_column_index, aggregatee_column_indices, result[2]))
        elif axis == 1:
            # column wise
            column_wise_results = [result for result in aggrdet_results if result[3] == AggregationDirection.COLUMN_WISE.value]
            for result in column_wise_results:
                aggregator_row_index = ast.literal_eval(result[0])[0]
                aggregatee_row_indices = tuple([ast.literal_eval(aggregatee_index)[0] for aggregatee_index in result[1]])
                aggregation_line_signatures.append((aggregator_row_index, aggregatee_row_indices, result[2]))
        aggregation_line_signatures = list(set(aggregation_line_signatures))
        return aggregation_line_signatures

    def mend_adjacent_aggregations(self, ar_cands_by_line, file_content, error_bound, axis):
        pass

    def is_equal(self, aggregator_value, aggregatees, based_aggregator_value, error_bound):
        pass

    def generate_ar_candidates_similar_headers(self):
        pass

    def detect_proximity_aggregation_relations(self, forest: AggregationRelationForest, error_bound: float, error_strategy):
        pass

    def setup_file_dicts(self, file_dicts, caller_name):
        files_dict_map = {}
        for file_dict in file_dicts:
            file_dict['detected_number_format'] = ''
            file_dict['detected_aggregations'] = []
            file_dict['exec_time'][caller_name] = math.nan

            files_dict_map[(file_dict['file_name'], file_dict['table_id'])] = file_dict
        return files_dict_map

