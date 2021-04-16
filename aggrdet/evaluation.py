# Created by lan at 2021/2/9
import json
import math
from copy import copy
from pprint import pprint

import luigi

from algorithm import Aggrdet
from bruteforce import Baseline
from database import store_experiment_result
from helpers import extract_dataset_name, is_aggregation_equal, AggregationOperator


class QualityEvaluation(luigi.Task):
    dataset_path = luigi.Parameter()
    algorithm = luigi.Parameter(default='Aggrdet')
    error_level = luigi.FloatParameter(default=0)
    target_aggregation_type = luigi.Parameter(default='All')
    error_strategy = luigi.Parameter(default='ratio')
    timeout = luigi.FloatParameter(default=300)
    use_extend_strategy = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    use_delayed_bruteforce = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    eval_only_aggor = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    verbose = luigi.BoolParameter(default=True, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    debug = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    result_path = luigi.Parameter(default='./debug/')

    def complete(self):
        return False

    def requires(self):
        if self.algorithm == 'Aggrdet':
            return Aggrdet(self.dataset_path, self.result_path, self.error_level, self.target_aggregation_type, self.error_strategy,
                           self.use_extend_strategy, self.use_delayed_bruteforce, debug=self.debug, timeout=self.timeout)
        elif self.algorithm == 'Baseline':
            return Baseline(dataset_path=self.dataset_path, result_path=self.result_path, error_level=self.error_level, timeout=self.timeout,
                            verbose=self.verbose, debug=self.debug)

    def run(self):
        if self.verbose:
            print("Parameter summary: \n Dataset path: %s \n Error level: %s \n Evaluate aggregator only: %s \n Use aggregation extension strategy: %s "
                  "\n Timeout: %s \n Use delayed brute-force strategy: %s" %
                  (self.dataset_path, self.error_level, self.eval_only_aggor, self.use_extend_strategy, self.timeout, self.use_delayed_bruteforce))
        results = []
        with self.input().open('r') as file_reader:
            results_dict = [json.loads(line) for line in file_reader]

        for result_dict in results_dict:
            # skip the files that are timed out
            exec_time = result_dict['exec_time'].values()
            if any([et == math.nan for et in exec_time]):
                continue

            ground_truth = result_dict['aggregation_annotations']
            pred_raw = result_dict['detected_aggregations']
            pred = []
            for ar in pred_raw:
                aggregator_index = ar['aggregator_index']
                aggregatee_indices = ar['aggregatee_indices']
                operator = ar['operator']
                pred.append([aggregator_index, aggregatee_indices, operator])
            count_true_positive = 0
            count_false_negative = 0
            count_tp_only_aggor = 0
            count_fn_only_aggor = 0

            # true_positive_cases = []
            true_positive_cases = {k: [] for k in AggregationOperator.all()}
            # false_negative_cases = []
            false_negative_cases = {k: [] for k in AggregationOperator.all()}
            # tp_cases_only_aggor = []
            tp_cases_only_aggor = {k: [] for k in AggregationOperator.all()}
            # fn_cases_only_aggor = []
            fn_cases_only_aggor = {k: [] for k in AggregationOperator.all()}

            if bool(ground_truth):
                for gt in ground_truth:
                    gt_aggor = gt['aggregator_index']
                    gt_aggees = gt['aggregatee_indices']
                    gt_operator = gt['operator']
                    if not (gt_operator in AggregationOperator.all()):
                        continue
                    # Todo: this does not consider the hard empty cells. May cause difference between ground-truth and prediction, even though they are the same.
                    if gt_operator == AggregationOperator.SUM.value:
                        match_only_aggor = [pd for pd in pred if pd[0] == gt_aggor and pd[2] == gt_operator]
                        match = [pd for pd in pred if
                                 is_aggregation_equal((gt_aggor, gt_aggees, gt_operator), (pd[0], pd[1], pd[2]), result_dict['table_array'])]
                    elif gt_operator == AggregationOperator.SUBTRACT.value:
                        transformed_aggor = gt_aggees[0]
                        transformed_aggee = copy(gt_aggees)
                        transformed_aggee[0] = gt['aggregator_index']
                        transformed_aggee = sorted(transformed_aggee)
                        match = [pd for pd in pred if pd[0] == transformed_aggor and sorted(pd[1]) == transformed_aggee and pd[2] == AggregationOperator.SUM.value]
                        match_only_aggor = [pd for pd in pred if pd[0] == transformed_aggor and pd[2] == gt_operator]
                    elif gt_operator == AggregationOperator.AVERAGE.value:
                        match = [pd for pd in pred if
                                 is_aggregation_equal((gt_aggor, gt_aggees, gt_operator), (pd[0], pd[1], pd[2]), result_dict['table_array'])]
                        match_only_aggor = [pd for pd in pred if pd[0] == gt_aggor and pd[2] == gt_operator]
                    elif gt_operator == AggregationOperator.PERCENTAGE.value:
                        # Todo: not implemented yet.
                        continue
                    else:
                        raise RuntimeError('Should not arrive here...')

                    if match:
                        count_true_positive += 1
                        if gt_operator not in true_positive_cases:
                            true_positive_cases[gt_operator] = []
                        true_positive_cases[gt_operator].append(gt)
                    else:
                        count_false_negative += 1
                        if gt_operator not in false_negative_cases:
                            false_negative_cases[gt_operator] = []
                        false_negative_cases[gt_operator].append(gt)
                    if match_only_aggor:
                        count_tp_only_aggor += 1
                        if gt_operator not in tp_cases_only_aggor:
                            tp_cases_only_aggor[gt_operator] = []
                        tp_cases_only_aggor[gt_operator].append(gt)
                    else:
                        count_fn_only_aggor += 1
                        if gt_operator not in fn_cases_only_aggor:
                            fn_cases_only_aggor[gt_operator] = []
                        fn_cases_only_aggor[gt_operator].append(gt)

            true_positive_cases[AggregationOperator.SUM.value] = true_positive_cases[AggregationOperator.SUM.value] + true_positive_cases[
                AggregationOperator.SUBTRACT.value]
            false_negative_cases[AggregationOperator.SUM.value] = false_negative_cases[AggregationOperator.SUM.value] + false_negative_cases[
                AggregationOperator.SUBTRACT.value]
            tp_cases_only_aggor[AggregationOperator.SUM.value] = tp_cases_only_aggor[AggregationOperator.SUM.value] + tp_cases_only_aggor[
                AggregationOperator.SUBTRACT.value]
            fn_cases_only_aggor[AggregationOperator.SUM.value] = fn_cases_only_aggor[AggregationOperator.SUM.value] + fn_cases_only_aggor[
                AggregationOperator.SUBTRACT.value]
            true_positive_cases.pop(AggregationOperator.SUBTRACT.value)
            false_negative_cases.pop(AggregationOperator.SUBTRACT.value)
            tp_cases_only_aggor.pop(AggregationOperator.SUBTRACT.value)
            fn_cases_only_aggor.pop(AggregationOperator.SUBTRACT.value)

            # false_positive_cases = []
            false_positive_cases = {k: [] for k in AggregationOperator.all()}
            # fp_cases_only_aggor = []
            fp_cases_only_aggor = {k: [] for k in AggregationOperator.all()}
            if bool(pred):
                for p in pred:
                    pred_aggor = p[0]
                    pred_aggees = p[1]
                    pred_operator = p[2]
                    matches = []
                    matches_only_aggor = []
                    for gt in ground_truth:
                        gt_operator = gt['operator']
                        if not (gt_operator in AggregationOperator.all()):
                            continue
                        if gt['operator'] == AggregationOperator.SUM.value:
                            match = is_aggregation_equal((gt['aggregator_index'], gt['aggregatee_indices'], gt_operator),
                                                         (pred_aggor, pred_aggees, pred_operator),
                                                         result_dict['table_array'])
                            match_only_aggor = pred_aggor == gt['aggregator_index'] and pred_operator == gt_operator
                        elif gt_operator == AggregationOperator.SUBTRACT.value:
                            transformed_aggor = gt['aggregatee_indices'][0]
                            transformed_aggees = [gt['aggregatee_indices'][1], gt['aggregator_index']]
                            match = pred_aggor == transformed_aggor and pred_aggees == sorted(transformed_aggees) and pred_operator == AggregationOperator.SUM.value
                            match_only_aggor = pred_aggor == transformed_aggor and pred_operator == gt_operator
                        elif gt_operator == AggregationOperator.AVERAGE.value:
                            match = is_aggregation_equal((gt['aggregator_index'], gt['aggregatee_indices'], gt_operator),
                                                         (pred_aggor, pred_aggees, pred_operator),
                                                         result_dict['table_array'])
                            match_only_aggor = pred_aggor == gt['aggregator_index'] and pred_operator == gt_operator
                        elif gt_operator == AggregationOperator.PERCENTAGE.value:
                            # Todo: not implemented yet.
                            continue
                        else:
                            raise RuntimeError('Should not arrive here...')
                        matches.append(match)
                        matches_only_aggor.append(match_only_aggor)
                    if not any(matches):
                        # Todo: false positives have different format from false negatives and true positives.
                        if pred_operator not in false_positive_cases:
                            false_positive_cases[pred_operator] = []
                        false_positive_cases[pred_operator].append(p)
                    if not any(matches_only_aggor):
                        if pred_operator not in fp_cases_only_aggor:
                            fp_cases_only_aggor[pred_operator] = []
                        fp_cases_only_aggor[pred_operator].append(p)
            false_positive_cases.pop(AggregationOperator.SUBTRACT.value)
            fp_cases_only_aggor.pop(AggregationOperator.SUBTRACT.value)

            result = copy(result_dict)
            result.pop('aggregation_detection_result', None)
            result['correct'] = true_positive_cases
            result['incorrect'] = false_negative_cases
            result['false_positive'] = false_positive_cases
            result['tp_only_aggor'] = tp_cases_only_aggor
            result['fn_only_aggor'] = fn_cases_only_aggor
            result['fp_only_aggor'] = fp_cases_only_aggor
            result['used_error_level'] = self.error_level
            results.append(result)

            if len(true_positive_cases.keys()) == 4:
                print(result['file_name'])

        # write experiment result into database
        if not self.debug:
            ds_name = extract_dataset_name(self.dataset_path)
            store_experiment_result(results, ds_name, self.eval_only_aggor, self.target_aggregation_type)


if __name__ == '__main__':
    luigi.run()
