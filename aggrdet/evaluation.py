# Created by lan at 2021/2/9
import json
import os
from copy import copy

import luigi
import yaml

from algorithm import Aggrdet
from bruteforce import Baseline
from database import store_experiment_result
from definitions import ROOT_DIR


class QualityEvaluation(luigi.Task):

    dataset_path = luigi.Parameter()
    algorithm = luigi.Parameter(default='Aggrdet')
    result_path = luigi.Parameter(default='./temp/')
    error_level = luigi.FloatParameter(default=0)
    satisfied_vote_ratio = luigi.FloatParameter(default=0.5)
    error_strategy = luigi.Parameter(default='ratio')
    timeout = luigi.FloatParameter(default=300)
    use_extend_strategy = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    db_config = luigi.DictParameter(default={'host': 'localhost', 'database': 'aggrdet', 'user': 'aggrdet', 'password': '123456'})
    verbose = luigi.BoolParameter(default=True, parsing=luigi.BoolParameter.EXPLICIT_PARSING)

    def complete(self):
        return False

    def output(self):
        # return luigi.LocalTarget(os.path.join(self.result_path, self.concat_output_file_name()))
        pass

    def requires(self):
        if self.algorithm == 'Aggrdet':
            return Aggrdet(self.dataset_path, self.result_path, self.error_level, self.satisfied_vote_ratio, self.error_strategy, self.use_extend_strategy)
        elif self.algorithm == 'Baseline':
            return Baseline(dataset_path=self.dataset_path, error_level=self.error_level, timeout=self.timeout, verbose=self.verbose)

    def run(self):
        if self.verbose:
            print("Parameter summary: \n Dataset path: %s \n Result path: %s \n Error level: %s \n Error calculation strategy: %s \n Use aggregation extension stragety: %s" %
                  (self.dataset_path, self.result_path, self.error_level, self.error_strategy, self.use_extend_strategy))
        results = []
        with self.input().open('r') as file_reader:
            results_dict = [json.loads(line) for line in file_reader]

        for result_dict in results_dict:
            # print(result_dict['file_name'])
            # if result_dict['file_name'] != 'C10004':
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
            if ground_truth is not None:
                for gt in ground_truth:
                    aggor = gt['aggregator_index']
                    aggees = sorted(gt['aggregatee_indices'])
                    # error_level = gt['error_bound']
                    error_level = gt['error_level_percent']
                    if not (gt['operator'] == 'Subtract' or gt['operator'] == 'Sum'):
                        continue
                    if gt['operator'] == 'Subtract':
                        aggor = aggees[0]
                        aggees[0] = gt['aggregator_index']
                        aggees = sorted(aggees)
                        match = [pd for pd in pred if pd[0] == aggor and sorted(pd[1]) == aggees]
                    else:
                        match = [pd for pd in pred if pd[0] == aggor and sorted(pd[1]) == aggees]
                    if match:
                        count_true_positive += 1
                        true_positive_cases.append(gt)
                    else:
                        count_false_negative += 1
                        false_negative_cases.append(gt)
            false_positive_cases = []
            if pred is not None:
                for p in pred:
                    aggor = p[0]
                    aggees = p[1]
                    matches = []
                    for gt in ground_truth:
                        if gt['operator'] != 'Sum' and gt['operator'] != 'Subtract':
                            continue
                        if gt['operator'] == 'Sum':
                            match = aggor == gt['aggregator_index'] and sorted(aggees) == sorted(gt['aggregatee_indices'])
                        else:
                            gt_aggor = gt['aggregatee_indices'][0]
                            gt_aggees = [gt['aggregatee_indices'][1], gt['aggregator_index']]
                            match = aggor == gt_aggor and aggees == sorted(gt_aggees)
                        matches.append(match)
                    if not any(matches):
                        false_positive_cases.append(p)

            result = copy(result_dict)
            result.pop('aggregation_detection_result', None)
            result['correct'] = true_positive_cases
            result['incorrect'] = false_negative_cases
            result['false_positive'] = false_positive_cases
            result['used_error_level'] = self.error_level
            results.append(result)

        # write experiment result into database
        with open(os.path.join(ROOT_DIR, '../config.yaml'), 'r') as stream:
            config = yaml.safe_load(stream)
            host = config['instances'][0]['host']
            database = config['instances'][0]['dbname']
            user = config['instances'][0]['username']
            password = config['instances'][0]['password']
            port = config['instances'][0]['port']

        import re
        m = re.search(r'.*/(.+).jl.gz', self.dataset_path)
        ds_name = None
        if isinstance(m, re.Match):
            try:
                ds_name = m.group(1)
            except IndexError:
                print('Extracting dataset name failed')
                exit(1)

        store_experiment_result(results, ds_name, host, database, user, password, port)

        # with self.output().open('w') as file_writer:
        #     for eval in results:
        #         file_writer.write(json.dumps(eval) + '\n')


if __name__ == '__main__':
    luigi.run()
