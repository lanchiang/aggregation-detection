# Created by lan at 2021/4/20
from eval._QualityEval import QualityEval
from helpers import AggregationOperator, is_aggregation_equal, hard_empty_cell_values


class RelativeChangeQualityEval(QualityEval):

    def __init__(self, file_dicts) -> None:
        super().__init__(file_dicts)
        self.operator = AggregationOperator.RELATIVE_CHANGE.value

    def extract_single_type_ground_truth(self):
        return {(file_dict['file_name'], file_dict['table_id']):
                    [annotation for annotation in file_dict['aggregation_annotations'] if annotation['operator'] == AggregationOperator.RELATIVE_CHANGE.value]
                for file_dict in self.file_dicts}

    def extract_single_type_predictions(self):
        return {key: [prediction for prediction in predictions if prediction['operator'] == AggregationOperator.RELATIVE_CHANGE.value]
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

                # Given the original gt as A=(B-C)/C, alternative gt is A=(C-B)/B
                alternative_gts = [(gt_aggregator, gt_aggregatees),
                                   (gt_aggregator, [gt_aggregatees[1], gt_aggregatees[0]])]

                # match_aggregator_only = gt_aggregator in [p['aggregator_index'] for p in self.single_type_predictions[file_id]]
                match_aggregator_only = False
                for pred in self.single_type_predictions[file_id]:
                    if any([True for alternative_gt in alternative_gts if alternative_gt[0] == pred['aggregator_index']]):
                        match_aggregator_only = True
                        break

                match = False
                for pred in self.single_type_predictions[file_id]:
                    if any([True for alternative_gt in alternative_gts if self.is_aggregation_equal(
                            (alternative_gt[0], alternative_gt[1], AggregationOperator.RELATIVE_CHANGE.value),
                            (pred['aggregator_index'], pred['aggregatee_indices'], AggregationOperator.RELATIVE_CHANGE.value),
                            self.file_values[file_id]
                    )]):
                        match = True
                        break

                self.true_positives_only_aggregator[file_id].append(gt) if match_aggregator_only else self.false_negatives_only_aggregator[file_id].append(gt)
                self.true_positives[file_id].append(gt) if match else self.false_negatives[file_id].append(gt)

    def _match_predictions(self):
        for file_id, file_predictions in self.single_type_predictions.items():
            self.false_positives_only_aggregator[file_id] = []
            self.false_positives[file_id] = []
            for prediction in file_predictions:
                pred_aggregator = prediction['aggregator_index']
                pred_aggregatees = prediction['aggregatee_indices']

                # Given the original prediction as A=(B-C)/C, alternative prediction is A=(C-B)/B
                alternative_preds = [(pred_aggregator, pred_aggregatees),
                                     (pred_aggregator, [pred_aggregatees[1], pred_aggregatees[0]])]

                # match_aggregator_only = gt_aggregator in [p['aggregator_index'] for p in self.single_type_predictions[file_id]]
                match_aggregator_only = False
                for gt in self.ground_truth[file_id]:
                    if any([True for alternative_pred in alternative_preds if alternative_pred[0] == gt['aggregator_index']]):
                        match_aggregator_only = True
                        break

                match = False
                for gt in self.ground_truth[file_id]:
                    if any([True for alternative_pred in alternative_preds if self.is_aggregation_equal(
                            (gt['aggregator_index'], gt['aggregatee_indices'], AggregationOperator.RELATIVE_CHANGE.value),
                            (alternative_pred[0], alternative_pred[1], AggregationOperator.RELATIVE_CHANGE.value),
                            self.file_values[file_id]
                    )]):
                        match = True
                        break

                if not match_aggregator_only:
                    self.false_positives_only_aggregator[file_id].append(prediction)
                if not match:
                    self.false_positives[file_id].append(prediction)

    def is_aggregation_equal(self, groundtruth, prediction, file_values) -> bool:
        """
        Check if two aggregations are equal to each other. Two aggregations are equal, if:
        1) the aggregators are the same
        2) the aggregatees are the same, or the difference set from prediction to groundtruth (prediction - groundtruth) contains only empty cells.

        :param groundtruth: the groundtruth aggregation, a 2-er tuple of (aggregator_index, aggregatee_indices)
        :param prediction: the prediction aggregation, a 2-er tuple of (aggregator_index, aggregatee_indices)
        :param file_values: values of the file, used to check intersections.
        :return: true if two aggregations are equal, false otherwise
        """
        if groundtruth[0] != prediction[0]:
            return False
        if groundtruth[2] != prediction[2]:
            return False

        groundtruth_aggregatee_indices = groundtruth[1]
        prediction_aggregatee_indices = prediction[1]
        if groundtruth_aggregatee_indices == prediction_aggregatee_indices:
            return True

        groundtruth_aggregatee_indices = [tuple(e) for e in groundtruth_aggregatee_indices]
        prediction_aggregatee_indices = [tuple(e) for e in prediction_aggregatee_indices]

        def diff(l1, l2):
            return list(set(l1) - set(l2))

        diff_set = diff(prediction_aggregatee_indices, groundtruth_aggregatee_indices)

        is_equal = True
        for d in diff_set:
            if file_values[d[0]][d[1]] not in hard_empty_cell_values:
                is_equal = False
                break
        return is_equal
