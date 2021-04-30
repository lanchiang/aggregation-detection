# Created by lan at 2021/4/28
import json
import os
from abc import ABC

import luigi
from luigi.mock import MockTarget

from approach.CollectiveAggregationDetectionTask import CollectiveAggregationDetectionTask
from approach.aggrdet.individual._AverageDetection import AverageDetection
from approach.aggrdet.individual._DivisionDetection import DivisionDetection
from approach.aggrdet.individual._RelativeChangeDetection import RelativeChangeDetection
from approach.aggrdet.supplemental._SupplementalSumDetection import SupplementalSumDetection
from approach.approach import AggregationDetection
from helpers import AggregationOperator


class SupplementalAggregationDetectionTask(luigi.Task):
    """
    After aggregation results of different operators are fused, apply some extra rules on these results to further retrieve true positive aggregations
    (supplemental aggregations) that cannot be found in the first stage.
    """
    dataset_path = luigi.Parameter()
    result_path = luigi.Parameter('./debug/')
    error_level_dict = luigi.DictParameter(default={'Sum': 0, 'Average': 0, 'Division': 0, 'RelativeChange': 0})
    target_aggregation_type = luigi.Parameter(default='All')
    error_strategy = luigi.Parameter(default='ratio')
    use_extend_strategy = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    use_delayed_bruteforce = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    timeout = luigi.FloatParameter(default=300)
    debug = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)

    def output(self):
        if self.debug:
            return luigi.LocalTarget(os.path.join(self.result_path, 'supplemental-aggregation-detection.jl'))
        else:
            return MockTarget('supplemental-aggregation-detection')

    def requires(self):
        sum_detector = {'sum_detector': SupplementalSumDetection(dataset_path=self.dataset_path, result_path=self.result_path,
                                                                 error_level_dict=self.error_level_dict,
                                                                 target_aggregation_type=self.target_aggregation_type,
                                                                 use_extend_strategy=self.use_extend_strategy,
                                                                 use_delayed_bruteforce=self.use_delayed_bruteforce, timeout=self.timeout, debug=self.debug)}
        # Todo: replace with supplemental version
        average_detector = {'average_detector': AverageDetection(dataset_path=self.dataset_path, result_path=self.result_path,
                                                                 error_level_dict=self.error_level_dict,
                                                                 target_aggregation_type=self.target_aggregation_type,
                                                                 use_extend_strategy=self.use_extend_strategy,
                                                                 use_delayed_bruteforce=self.use_delayed_bruteforce, timeout=self.timeout,
                                                                 debug=self.debug)}
        division_detector = {
            'division_detector': DivisionDetection(dataset_path=self.dataset_path, result_path=self.result_path,
                                                   error_level_dict=self.error_level_dict, target_aggregation_type=self.target_aggregation_type,
                                                   use_extend_strategy=self.use_extend_strategy, use_delayed_bruteforce=self.use_delayed_bruteforce,
                                                   timeout=self.timeout, debug=self.debug)}
        relative_change_detector = {
            'relative_change_detector': RelativeChangeDetection(dataset_path=self.dataset_path, result_path=self.result_path,
                                                                error_level_dict=self.error_level_dict,
                                                                target_aggregation_type=self.target_aggregation_type,
                                                                use_extend_strategy=self.use_extend_strategy,
                                                                use_delayed_bruteforce=self.use_delayed_bruteforce,
                                                                timeout=self.timeout, debug=self.debug)}

        all_detectors = {**sum_detector, **average_detector, **division_detector, **relative_change_detector}

        required = {AggregationOperator.SUM.value: sum_detector,
                    AggregationOperator.AVERAGE.value: average_detector,
                    AggregationOperator.DIVISION.value: division_detector,
                    AggregationOperator.RELATIVE_CHANGE.value: relative_change_detector,
                    'All': all_detectors}.get(self.target_aggregation_type, None)

        if required is None:
            raise RuntimeError('Given target aggregation type parameter is illegal.')

        return required

    def run(self):
        with self.input().open('r') as file_reader:
            fused_aggregation_results = [json.loads(line) for line in file_reader]

        # what does this task do?
        # 1) Remove other types of aggregations, and do the individual detection of this operator
        # 2) Apply individual detection of this operator on all other Sum, division results (all combinations)

        with self.output().open('w') as file_writer:
            for file_dict in fused_aggregation_results:
                file_writer.write(json.dumps(file_dict) + '\n')
        pass
