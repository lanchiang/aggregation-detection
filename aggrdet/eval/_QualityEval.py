# Created by lan at 2021/4/19
import math
from abc import ABC, abstractmethod


class QualityEval(ABC):

    def __init__(self, file_dicts) -> None:
        super().__init__()
        # keep only those files the approach finish parse within timeout.
        self.file_dicts = [file_dict for file_dict in file_dicts if not any([exec_time == math.nan for exec_time in file_dict['exec_time'].values()])]
        self.indexed_file_dicts = {(file_dict['file_name'], file_dict['table_id']): file_dict for file_dict in file_dicts}

        # set ground truth
        self.ground_truth = self.extract_single_type_ground_truth()

        # set predictions
        self.predictions = {(file_dict['file_name'], file_dict['table_id']): file_dict['detected_aggregations'] for file_dict in self.file_dicts}

        self.single_type_predictions = self.extract_single_type_predictions()

        self.file_values = {(file_dict['file_name'], file_dict['table_id']): file_dict['table_array'] for file_dict in self.file_dicts}

        self.true_positives = {}
        self.true_positives_only_aggregator = {}
        self.false_negatives = {}
        self.false_negatives_only_aggregator = {}
        self.false_positives = {}
        self.false_positives_only_aggregator = {}

        self.operator = None

    @abstractmethod
    def extract_single_type_ground_truth(self):
        pass

    @abstractmethod
    def extract_single_type_predictions(self):
        pass

    @abstractmethod
    def _match_ground_truth(self):
        pass

    @abstractmethod
    def _match_predictions(self):
        pass

    def analyze_results(self):
        self._match_ground_truth()
        self._match_predictions()

        for file_signature, file_dict in self.indexed_file_dicts.items():
            file_dict.pop('aggregation_detection_result', None)
            if 'correct' not in file_dict:
                file_dict['correct'] = {}
            file_dict['correct'][self.operator] = self.true_positives[file_signature]

            if 'incorrect' not in file_dict:
                file_dict['incorrect'] = {}
            file_dict['incorrect'][self.operator] = self.false_negatives[file_signature]

            if 'false_positive' not in file_dict:
                file_dict['false_positive'] = {}
            file_dict['false_positive'][self.operator] = self.false_positives[file_signature]

            if 'tp_only_aggor' not in file_dict:
                file_dict['tp_only_aggor'] = {}
            file_dict['tp_only_aggor'][self.operator] = self.true_positives_only_aggregator[file_signature]

            if 'fn_only_aggor' not in file_dict:
                file_dict['fn_only_aggor'] = {}
            file_dict['fn_only_aggor'][self.operator] = self.false_negatives_only_aggregator[file_signature]

            if 'fp_only_aggor' not in file_dict:
                file_dict['fp_only_aggor'] = {}
            file_dict['fp_only_aggor'][self.operator] = self.false_positives_only_aggregator[file_signature]