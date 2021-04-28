# Created by lan at 2021/4/20
from eval._QualityEval import QualityEval
from helpers import AggregationOperator, is_aggregation_equal


class DivisionQualityEval(QualityEval):

    def __init__(self, file_dicts) -> None:
        super().__init__(file_dicts)
        self.operator = AggregationOperator.DIVISION.value

    def extract_single_type_ground_truth(self):
        return {(file_dict['file_name'], file_dict['table_id']):
                    [annotation for annotation in file_dict['aggregation_annotations'] if annotation['operator'] == AggregationOperator.DIVISION.value]
                for file_dict in self.file_dicts}

    def extract_single_type_predictions(self):
        return {key: [prediction for prediction in predictions if prediction['operator'] == AggregationOperator.DIVISION.value]
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

                # Given the original gt as A=B/C, alternative gts are A=C/B, B=C/A, C=B/A
                alternative_gts = [(gt_aggregator, gt_aggregatees),
                                   # (gt_aggregator, [gt_aggregatees[1], gt_aggregatees[0]]),
                                   # (gt_aggregatees[0], [gt_aggregatees[1], gt_aggregator]),
                                   (gt_aggregatees[1], [gt_aggregatees[0], gt_aggregator])]

                # match_aggregator_only = gt_aggregator in [p['aggregator_index'] for p in self.single_type_predictions[file_id]]
                match_aggregator_only = False
                for pred in self.single_type_predictions[file_id]:
                    if any([True for alternative_gt in alternative_gts if alternative_gt[0] == pred['aggregator_index']]):
                        match_aggregator_only = True
                        break

                match = False
                for pred in self.single_type_predictions[file_id]:
                    if any([True for alternative_gt in alternative_gts if is_aggregation_equal(
                            (alternative_gt[0], alternative_gt[1], AggregationOperator.DIVISION.value),
                            (pred['aggregator_index'], pred['aggregatee_indices'], AggregationOperator.DIVISION.value),
                            self.file_values[file_id]
                    )]):
                        match = True
                        break
                    # for alternative_gt in alternative_gts:
                    #     if is_aggregation_equal(
                    #             (alternative_gt[0], alternative_gt[1], AggregationOperator.DIVISION.value),
                    #             (pred['aggregator_index'], pred['aggregatee_indices'], AggregationOperator.DIVISION.value),
                    #             self.file_values[file_id]
                    #     ):
                    #         match = True
                    #         break
                    # if match:
                    #     break

                # match = bool(
                #     [p for p in self.single_type_predictions[file_id] if
                #      is_aggregation_equal((gt_aggregator, gt_aggregatees, AggregationOperator.DIVISION.value),
                #                           (p['aggregator_index'], p['aggregatee_indices'],
                #                            AggregationOperator.DIVISION.value),
                #                           self.file_values[file_id])])

                self.true_positives_only_aggregator[file_id].append(gt) if match_aggregator_only else self.false_negatives_only_aggregator[file_id].append(gt)
                self.true_positives[file_id].append(gt) if match else self.false_negatives[file_id].append(gt)

    def _match_predictions(self):
        for file_id, file_predictions in self.single_type_predictions.items():
            self.false_positives_only_aggregator[file_id] = []
            self.false_positives[file_id] = []
            for prediction in file_predictions:
                pred_aggregator = prediction['aggregator_index']
                pred_aggregatees = prediction['aggregatee_indices']

                # Given the original prediction as A=B/C, alternative predictions are A=C/B, B=C/A, C=B/A
                alternative_preds = [(pred_aggregator, pred_aggregatees),
                                     # (pred_aggregator, [pred_aggregatees[1], pred_aggregatees[0]]),
                                     # (pred_aggregatees[0], [pred_aggregatees[1], pred_aggregator]),
                                     (pred_aggregatees[1], [pred_aggregatees[0], pred_aggregator])]

                # match_aggregator_only = gt_aggregator in [p['aggregator_index'] for p in self.single_type_predictions[file_id]]
                match_aggregator_only = False
                for gt in self.ground_truth[file_id]:
                    if any([True for alternative_pred in alternative_preds if alternative_pred[0] == gt['aggregator_index']]):
                        match_aggregator_only = True
                        break

                match = False
                for gt in self.ground_truth[file_id]:
                    if any([True for alternative_pred in alternative_preds if is_aggregation_equal(
                            (gt['aggregator_index'], gt['aggregatee_indices'], AggregationOperator.DIVISION.value),
                            (alternative_pred[0], alternative_pred[1], AggregationOperator.DIVISION.value),
                            self.file_values[file_id]
                    )]):
                        match = True
                        break

                # match_aggregator_only = pred_aggregator in [gt['aggregator_index'] for gt in self.ground_truth[file_id]]
                # match = bool([gt for gt in self.ground_truth[file_id] if
                #               is_aggregation_equal((gt['aggregator_index'], gt['aggregatee_indices'], AggregationOperator.AVERAGE.value),
                #                                    (pred_aggregator, pred_aggregatees, AggregationOperator.AVERAGE.value),
                #                                    self.file_values[file_id])])

                if not match_aggregator_only:
                    self.false_positives_only_aggregator[file_id].append(prediction)
                if not match:
                    self.false_positives[file_id].append(prediction)
