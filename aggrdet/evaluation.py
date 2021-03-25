# Created by lan at 2021/2/9
import json
from copy import copy

import luigi

from algorithm import Aggrdet
from bruteforce import Baseline
from database import store_experiment_result
from helpers import extract_dataset_name, is_aggregation_equal


class QualityEvaluation(luigi.Task):

    dataset_path = luigi.Parameter()
    algorithm = luigi.Parameter(default='Aggrdet')
    error_level = luigi.FloatParameter(default=0)
    error_strategy = luigi.Parameter(default='ratio')
    timeout = luigi.FloatParameter(default=300)
    use_extend_strategy = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    use_delayed_bruteforce = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    verbose = luigi.BoolParameter(default=True, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    debug = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    result_path = luigi.Parameter(default='./debug/')

    def complete(self):
        return False

    def requires(self):
        if self.algorithm == 'Aggrdet':
            return Aggrdet(self.dataset_path, self.result_path, self.error_level, self.error_strategy,
                            self.use_extend_strategy, self.use_delayed_bruteforce, debug=self.debug, timeout=self.timeout)
        elif self.algorithm == 'Baseline':
            return Baseline(dataset_path=self.dataset_path, result_path=self.result_path, error_level=self.error_level, timeout=self.timeout, verbose=self.verbose, debug=self.debug)

    def run(self):
        if self.verbose:
            print("Parameter summary: \n Dataset path: %s \n Error level: %s \n Error calculation strategy: %s \n Use aggregation extension strategy: %s "
                  "\n Timeout: %s \n Use delayed brute-force strategy: %s" %
                  (self.dataset_path, self.error_level, self.error_strategy, self.use_extend_strategy, self.timeout, self.use_delayed_bruteforce))
        results = []
        with self.input().open('r') as file_reader:
            results_dict = [json.loads(line) for line in file_reader]

        for result_dict in results_dict:
            # skip the files that are timed out
            exec_time = result_dict['exec_time'].values()
            if any([et < 0 for et in exec_time]):
                continue

            ground_truth = result_dict['aggregation_annotations']
            pred_raw = result_dict['detected_aggregations']
            pred = []
            # print(result_dict['file_name'])
            for ar in pred_raw:
                aggregator_index = ar['aggregator_index']
                aggregatee_indices = ar['aggregatee_indices']
                pred.append([aggregator_index, aggregatee_indices])
            count_true_positive = 0
            count_false_negative = 0
            true_positive_cases = []
            false_negative_cases = []
            if bool(ground_truth):
                for gt in ground_truth:
                    aggor = gt['aggregator_index']
                    aggees = sorted(gt['aggregatee_indices'])
                    if not (gt['operator'] == 'Subtract' or gt['operator'] == 'Sum'):
                        continue
                    # Todo: this does not consider the hard empty cells. May cause difference between groundtruth and prediction, even though they are the same.
                    if gt['operator'] == 'Subtract':
                        aggor = aggees[0]
                        aggees[0] = gt['aggregator_index']
                        aggees = sorted(aggees)
                        match = [pd for pd in pred if pd[0] == aggor and sorted(pd[1]) == aggees]
                    else:
                        # match = [pd for pd in pred if pd[0] == aggor and sorted(pd[1]) == aggees]
                        match = [pd for pd in pred if is_aggregation_equal((aggor, aggees), (pd[0], pd[1]), result_dict['table_array'])]
                    if match:
                        count_true_positive += 1
                        true_positive_cases.append(gt)
                    else:
                        count_false_negative += 1
                        false_negative_cases.append(gt)
            false_positive_cases = []
            if bool(pred):
                for p in pred:
                    aggor = p[0]
                    aggees = p[1]
                    matches = []
                    for gt in ground_truth:
                        if gt['operator'] != 'Sum' and gt['operator'] != 'Subtract':
                            continue
                        if gt['operator'] == 'Sum':
                            match = is_aggregation_equal((gt['aggregator_index'], gt['aggregatee_indices']), (aggor, aggees), result_dict['table_array'])
                        else:
                            gt_aggor = gt['aggregatee_indices'][0]
                            gt_aggees = [gt['aggregatee_indices'][1], gt['aggregator_index']]
                            match = aggor == gt_aggor and aggees == sorted(gt_aggees)
                        matches.append(match)
                    if not any(matches):
                        # Todo: false positives have different format from false negatives and true positives.
                        false_positive_cases.append(p)

            result = copy(result_dict)
            result.pop('aggregation_detection_result', None)
            result['correct'] = true_positive_cases
            result['incorrect'] = false_negative_cases
            result['false_positive'] = false_positive_cases
            result['used_error_level'] = self.error_level
            results.append(result)

        # write experiment result into database
        if not self.debug:
            ds_name = extract_dataset_name(self.dataset_path)
            store_experiment_result(results, ds_name)


if __name__ == '__main__':
    luigi.run()
