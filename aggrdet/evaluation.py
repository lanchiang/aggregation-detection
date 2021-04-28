# Created by lan at 2021/2/9
import itertools
import json

import luigi

from algorithm import Aggrdet
from bruteforce import Baseline
from database import store_experiment_result
from eval._AverageQualityEval import AverageQualityEval
from eval._DivisionQualityEval import DivisionQualityEval
from eval._RelativeChangeEval import RelativeChangeQualityEval
from eval._SumQualityEval import SumQualityEval
from helpers import extract_dataset_name, AggregationOperator


class QualityEvaluation(luigi.Task):
    dataset_path = luigi.Parameter()
    algorithm = luigi.Parameter(default='Aggrdet')
    # error_level = luigi.FloatParameter(default=0)
    error_level_dict = luigi.DictParameter()
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
            return Aggrdet(self.dataset_path, self.result_path, self.error_level_dict, self.target_aggregation_type, self.error_strategy,
                           self.use_extend_strategy, self.use_delayed_bruteforce, debug=self.debug, timeout=self.timeout)
        elif self.algorithm == 'Baseline':
            return Baseline(dataset_path=self.dataset_path, result_path=self.result_path, error_level=self.error_level_dict, timeout=self.timeout,
                            verbose=self.verbose, debug=self.debug)

    def run(self):
        if self.verbose:
            print("Parameter summary: \n Dataset path: %s \n Error level: %s \n Evaluate aggregator only: %s \n Use aggregation extension strategy: %s "
                  "\n Timeout: %s \n Use delayed brute-force strategy: %s \n Target aggregation type: %s" %
                  (self.dataset_path, self.error_level_dict, self.eval_only_aggor, self.use_extend_strategy, self.timeout,
                   self.use_delayed_bruteforce, self.target_aggregation_type))
        with self.input().open('r') as file_reader:
            results_dict = [json.loads(line) for line in file_reader]

        evaluators = self.__get_quality_evaluator(file_dicts=results_dict)

        for evaluator in evaluators:
            evaluator.analyze_results()

        # write experiment result into database
        if not self.debug:
            ds_name = extract_dataset_name(self.dataset_path)
            store_experiment_result(results_dict, ds_name, self.eval_only_aggor, self.target_aggregation_type)

    def __get_quality_evaluator(self, file_dicts):
        target_aggregator_type = self.target_aggregation_type
        evaluators = {
            AggregationOperator.SUM.value: [SumQualityEval(file_dicts)],
            AggregationOperator.AVERAGE.value: [AverageQualityEval(file_dicts)],
            AggregationOperator.DIVISION.value: [DivisionQualityEval(file_dicts)],
            AggregationOperator.RELATIVE_CHANGE.value: [RelativeChangeQualityEval(file_dicts)],
            'All': [SumQualityEval(file_dicts), AverageQualityEval(file_dicts), DivisionQualityEval(file_dicts), RelativeChangeQualityEval(file_dicts)]
        }.get(target_aggregator_type, None)

        if evaluators is None:
            raise KeyError('Given target aggregator type %s is illegal.' % self.target_aggregation_type)

        return evaluators


if __name__ == '__main__':
    luigi.run()

    # error_level_candidates = [0, 0.00005, 0.0005, 0.005, 0.05]
    #
    # sum_error_level_candidates = [0.00005, 0.0001, 0.0005, 0.001]
    # average_error_level_candidates = [0, 0.00001, 0.00005, 0.0001]
    # division_error_level_candidates = [0.00001, 0.00005, 0.0001, 0.0005]
    # relative_change_error_level_candidates = [0.005, 0.01, 0.05]
    #
    # error_level_products = itertools.product(sum_error_level_candidates, average_error_level_candidates, division_error_level_candidates, relative_change_error_level_candidates)
    #
    # error_level_dicts = []
    # for error_level_permutation in error_level_products:
    #     error_level_dict = json.dumps({'Sum': error_level_permutation[0], 'Average': error_level_permutation[1],
    #                                    'Division': error_level_permutation[2], 'RelativeChange': error_level_permutation[3]})
    #     error_level_dicts.append(error_level_dict)
    #     # luigi.run(
    #     #     cmdline_args=["--log-level=WARNING",
    #     #                   "--dataset-path=../data/dataset.jl.gz",
    #     #                   "--error-level=%s" % error_level_dict],
    #     #     main_task_cls=QualityEvaluation,
    #     #     local_scheduler=True)
    #
    # with open('./temp/error_level_dicts.txt', mode='w') as file_writer:
    #     for error_level_dict in error_level_dicts:
    #         file_writer.write(error_level_dict + '\n')
