# Created by lan at 2021/2/9
import json
from copy import copy

import luigi

from algorithm import SelectNumberFormat


class QualityEvaluation(luigi.Task):

    dataset_path = luigi.Parameter()
    error_bound = luigi.FloatParameter(default=10)
    satisfied_vote_ratio = luigi.FloatParameter(default=0.5)

    def output(self):
        return luigi.LocalTarget('temp/quality-eval.jl')

    def requires(self):
        return SelectNumberFormat(self.dataset_path, self.error_bound, self.satisfied_vote_ratio)

    def run(self):
        results = []
        with self.input().open('r') as file_reader:
            results_dict = [json.loads(line) for line in file_reader]

        for result_dict in results_dict:
            # print(result_dict['file_name'])
            # if result_dict['file_name'] != 'C10042':
            #     continue
            ground_truth = result_dict['aggregation_annotations'] if result_dict['aggregation_annotations'] is not None else []
            pred_raw = result_dict['detected_aggregations']
            pred = []
            for ar in pred_raw:
                aggregator_index = ar['aggregator_index']
                aggregatee_indices = ar['aggregatee_indices']
                pred.append([aggregator_index, aggregatee_indices])
            count_true_positive = 0
            count_false_negative = 0
            true_positive_cases = []
            false_negative_cases = []
            correct_cases_error_bounded = []
            incorrect_cases_error_bounded = []
            if ground_truth is not None:
                for gt in ground_truth:
                    aggor = gt['aggregator_index']
                    aggees = sorted(gt['aggregatee_indices'])
                    error_level = gt['error_bound']
                    if not (gt['operator'] == 'Subtract' or gt['operator'] == 'Sum'):
                        continue
                    if gt['operator'] == 'Subtract':
                        aggor = aggees[0]
                        aggees[0] = gt['aggregator_index']
                        aggees = sorted(aggees)
                        match1 = [pd for pd in pred if pd[0] == aggor and sorted(pd[1]) == aggees]
                        aggees = sorted(gt['aggregatee_indices'])
                        aggor = aggees[1]
                        aggees[1] = gt['aggregator_index']
                        aggees = sorted(aggees)
                        match2 = [pd for pd in pred if pd[0] == aggor and sorted(pd[1]) == aggees]
                        match = match1 or match2
                    else:
                        match = [pd for pd in pred if pd[0] == aggor and sorted(pd[1]) == aggees]
                    if match:
                        count_true_positive += 1
                        true_positive_cases.append(gt)
                    else:
                        count_false_negative += 1
                        false_negative_cases.append(gt)
                    if error_level <= self.error_bound:
                        if match:
                            correct_cases_error_bounded.append(gt)
                        else:
                            incorrect_cases_error_bounded.append(gt)
            false_positive_cases = []
            true_negative_cases = []
            if pred is not None:
                for p in pred:
                    aggor = p[0]
                    aggees = sorted(p[1])
                    matches = []
                    for gt in ground_truth:
                        if gt['operator'] != 'Sum' and gt['operator'] != 'Subtract':
                            continue
                        if gt['operator'] == 'Sum':
                            match = aggor == gt['aggregator_index'] and aggees == sorted(gt['aggregatee_indices'])
                        else:
                            gt_aggor = gt['aggregatee_indices'][0]
                            gt_aggees = [gt['aggregatee_indices'][1], gt['aggregator_index']]
                            match1 = aggor == gt_aggor and aggees == sorted(gt_aggees)
                            gt_aggor = gt['aggregatee_indices'][1]
                            gt_aggees = [gt['aggregatee_indices'][0], gt['aggregator_index']]
                            match2 = aggor == gt_aggor and aggees == sorted(gt_aggees)
                            match = match1 or match2
                        matches.append(match)
                    if not any(matches):
                        false_positive_cases.append(p)

            result = copy(result_dict)
            result.pop('aggregation_detection_result', None)
            result['correct'] = true_positive_cases
            result['incorrect'] = false_negative_cases
            result['false_positive'] = false_positive_cases
            result['correct_error_bounded'] = correct_cases_error_bounded
            result['incorrect_error_bounded'] = incorrect_cases_error_bounded
            result['false_positive_error_bounded'] = false_positive_cases
            result['used_error_level'] = self.error_bound
            results.append(result)

        with self.output().open('w') as file_writer:
            for eval in results:
                file_writer.write(json.dumps(eval) + '\n')

if __name__ == '__main__':
    luigi.run()