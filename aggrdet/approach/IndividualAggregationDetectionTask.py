# Created by lan at 2021/4/28
import math
from abc import ABC, abstractmethod

from approach.approach import AggregationDetection
from dataprep import NumberFormatNormalization


class IndividualAggregationDetection(AggregationDetection, ABC):

    def requires(self):
        return NumberFormatNormalization(self.dataset_path, self.result_path,
                                         self.error_level_dict, self.use_extend_strategy, self.use_delayed_bruteforce, self.timeout,
                                         debug=self.debug)

    @abstractmethod
    def mend_adjacent_aggregations(self, ar_cands_by_line, file_content, error_bound, axis):
        pass

    @abstractmethod
    def is_equal(self, aggregator_value, aggregatees, based_aggregator_value, error_bound):
        pass

    @abstractmethod
    def generate_ar_candidates_similar_headers(self):
        pass

    def setup_file_dicts(self, file_dicts, caller_name):
        files_dict_map = {}
        for file_dict in file_dicts:
            file_dict['detected_number_format'] = ''
            file_dict['detected_aggregations'] = []
            file_dict['aggregation_detection_result'] = {file_dict['number_format']: []}
            file_dict['exec_time'][caller_name] = math.nan

            files_dict_map[(file_dict['file_name'], file_dict['table_id'])] = file_dict
        return files_dict_map

