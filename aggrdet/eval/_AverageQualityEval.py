# Created by lan at 2021/4/19
from eval._QualityEval import QualityEval
from helpers import AggregationOperator, is_aggregation_equal


class AverageQualityEval(QualityEval):

    def __init__(self, file_dicts) -> None:
        super().__init__(file_dicts)
        self.operator = AggregationOperator.AVERAGE.value

    def extract_single_type_ground_truth(self):
        return {(file_dict['file_name'], file_dict['table_id']):
                    [annotation for annotation in file_dict['aggregation_annotations'] if annotation['operator'] == AggregationOperator.AVERAGE.value]
                for file_dict in self.file_dicts}

    def extract_single_type_predictions(self):
        return {key: [prediction for prediction in predictions if prediction['operator'] == AggregationOperator.AVERAGE.value]
                for key, predictions in self.predictions.items()}

    def _match_ground_truth(self):
        for file_id, file_gt in self.ground_truth.items():
            self.true_positives_only_aggregator[file_id] = []
            self.true_positives[file_id] = []
            self.false_negatives_only_aggregator[file_id] = []
            self.false_negatives[file_id] = []
            for gt in file_gt:
                gt_aggregator = gt['aggregator_index']
                gt_aggregatees = gt['aggregatee_indices']

                match_aggregator_only = gt_aggregator in [p['aggregator_index'] for p in self.single_type_predictions[file_id]]
                match = bool(
                    [p for p in self.single_type_predictions[file_id] if is_aggregation_equal((gt_aggregator, gt_aggregatees, AggregationOperator.AVERAGE.value),
                                                                                              (p['aggregator_index'], p['aggregatee_indices'],
                                                                                               AggregationOperator.AVERAGE.value),
                                                                                              self.file_values[file_id])])

                self.true_positives_only_aggregator[file_id].append(gt) if match_aggregator_only else self.false_negatives_only_aggregator[file_id].append(gt)
                self.true_positives[file_id].append(gt) if match else self.false_negatives[file_id].append(gt)

    def _match_predictions(self):
        for file_id, file_predictions in self.single_type_predictions.items():
            self.false_positives_only_aggregator[file_id] = []
            self.false_positives[file_id] = []
            for prediction in file_predictions:
                pred_aggregator = prediction['aggregator_index']
                pred_aggregatees = prediction['aggregatee_indices']

                match_aggregator_only = pred_aggregator in [gt['aggregator_index'] for gt in self.ground_truth[file_id]]
                match = bool([gt for gt in self.ground_truth[file_id] if
                              is_aggregation_equal((gt['aggregator_index'], gt['aggregatee_indices'], AggregationOperator.AVERAGE.value),
                                                   (pred_aggregator, pred_aggregatees, AggregationOperator.AVERAGE.value),
                                                   self.file_values[file_id])])

                if not match_aggregator_only:
                    self.false_positives_only_aggregator[file_id].append(prediction)
                if not match:
                    self.false_positives[file_id].append(prediction)
