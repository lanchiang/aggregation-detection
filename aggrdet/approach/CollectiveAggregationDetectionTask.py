# Created by lan at 2021/4/28
import ast
import itertools
import json
import os

import luigi
from luigi.mock import MockTarget

from approach.aggrdet.individual._AverageDetection import AverageDetectionTask
from approach.aggrdet.individual._DivisionDetection import DivisionDetection
from approach.aggrdet.individual._RelativeChangeDetection import RelativeChangeDetection
from approach.aggrdet.individual.sum import SumDetectionTask
from helpers import AggregationOperator, AggregationDirection


class CollectiveAggregationDetectionTask(luigi.Task):
    dataset_path = luigi.Parameter()
    result_path = luigi.Parameter('./debug/')
    error_level_dict = luigi.DictParameter(default={'Sum': 0, 'Average': 0, 'Division': 0, 'RelativeChange': 0})
    target_aggregation_type = luigi.Parameter(default='All')
    error_strategy = luigi.Parameter(default='ratio')
    use_extend_strategy = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    use_delayed_bruteforce = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    timeout = luigi.FloatParameter(default=300)

    debug = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)

    gathered_detection_results = None

    def output(self):
        if self.debug:
            return luigi.LocalTarget(os.path.join(self.result_path, 'fused-aggregation-results.jl'))
        else:
            return MockTarget('fused-aggregation-results')

    def requires(self):
        sum_detector = {'sum_detector': SumDetectionTask(dataset_path=self.dataset_path, result_path=self.result_path,
                                                         error_level_dict=self.error_level_dict,
                                                         use_extend_strategy=self.use_extend_strategy,
                                                         use_delayed_bruteforce=self.use_delayed_bruteforce, timeout=self.timeout, debug=self.debug)}
        average_detector = {'average_detector': AverageDetectionTask(dataset_path=self.dataset_path, result_path=self.result_path,
                                                                     error_level_dict=self.error_level_dict,
                                                                     use_extend_strategy=self.use_extend_strategy,
                                                                     use_delayed_bruteforce=self.use_delayed_bruteforce, timeout=self.timeout,
                                                                     debug=self.debug)}
        division_detector = {
            'division_detector': DivisionDetection(dataset_path=self.dataset_path, result_path=self.result_path,
                                                   error_level_dict=self.error_level_dict,
                                                   use_extend_strategy=self.use_extend_strategy, use_delayed_bruteforce=self.use_delayed_bruteforce,
                                                   timeout=self.timeout, debug=self.debug)}
        relative_change_detector = {
            'relative_change_detector': RelativeChangeDetection(dataset_path=self.dataset_path, result_path=self.result_path,
                                                                error_level_dict=self.error_level_dict,
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
        gathered_detection_results_by_file_signature = {}
        for key, _ in self.input().items():
            with self.input()[key].open('r') as file_reader:
                file_dicts = [json.loads(line) for line in file_reader]
                for file_dict in file_dicts:
                    file_signature = (file_dict['file_name'], file_dict['table_id'])
                    if file_signature not in gathered_detection_results_by_file_signature:
                        gathered_detection_results_by_file_signature[file_signature] = file_dict
                    else:
                        gathered = gathered_detection_results_by_file_signature[file_signature]['aggregation_detection_result']
                        for number_format in gathered.keys():
                            if number_format in gathered:
                                gathered[number_format].extend(file_dict['aggregation_detection_result'][number_format])
                            else:
                                gathered[number_format] = file_dict['aggregation_detection_result'][number_format]

        self.gathered_detection_results = list(gathered_detection_results_by_file_signature.values())
        self._fuse_aggregation_results()

        with self.output().open('w') as file_writer:
            for file_dict in self.gathered_detection_results:
                file_writer.write(json.dumps(file_dict) + '\n')

    def _fuse_aggregation_results(self):
        for file_dict in self.gathered_detection_results:
            file_aggrdet_results = file_dict['aggregation_detection_result']

            for number_format in file_aggrdet_results.keys():
                filtered_results = []
                # row wise fusion
                row_wise_aggr_results = [result for result in file_aggrdet_results[number_format] if result[3] == AggregationDirection.ROW_WISE.value]

                # group by operator and column signature. Column signature of an aggregation is the column index of aggregator and column indices of aggregatees
                result_grp_by_column_sign_operator = {}
                for aggrdet_result in row_wise_aggr_results:
                    aggregator_index = ast.literal_eval(aggrdet_result[0])
                    aggregatees_indices = [ast.literal_eval(aggregatee_index) for aggregatee_index in aggrdet_result[1]]
                    column_signature = (aggregator_index[1], tuple([e[1] for e in aggregatees_indices]))
                    operator = aggrdet_result[2]

                    signature = (column_signature, operator)
                    if signature not in result_grp_by_column_sign_operator:
                        result_grp_by_column_sign_operator[signature] = []
                    result_grp_by_column_sign_operator[signature].append(aggrdet_result)

                # order the result groups by 1) the length of aggregatee list; 2) the number of their detected aggregations
                # the higher the group is in the rank, the more detected aggregations in a group and the longer the aggregatee list is
                result_grp_by_column_sign_operator = {k: v for k, v in
                                                      sorted(result_grp_by_column_sign_operator.items(), key=lambda e: (len(e[0][0][1]), len(e[1])),
                                                             reverse=True)}
                filtered_results_row_wise = self.__filter_aggregations_by_signature(result_grp_by_column_sign_operator)
                filtered_results.extend(filtered_results_row_wise)

                # column wise fusion
                # aggrdet_result is a 4er-tuple, (Aggregator_index_string, Aggregatee_indices_strings, Operator_type, Aggregation_direction)
                column_wise_aggr_results = [result for result in file_aggrdet_results[number_format] if result[3] == AggregationDirection.COLUMN_WISE.value]

                # group by operator and row signature. Row signature of an aggregation is the row index of aggregator and row indices of aggregatees
                result_grp_by_row_sign_operator = {}
                for aggrdet_result in column_wise_aggr_results:
                    aggregator_index = ast.literal_eval(aggrdet_result[0])
                    aggregatees_indices = [ast.literal_eval(aggregatee_index) for aggregatee_index in aggrdet_result[1]]
                    row_signature = (aggregator_index[0], tuple([e[0] for e in aggregatees_indices]))
                    operator = aggrdet_result[2]

                    signature = (row_signature, operator)
                    if signature not in result_grp_by_row_sign_operator:
                        result_grp_by_row_sign_operator[signature] = []
                    result_grp_by_row_sign_operator[signature].append(aggrdet_result)

                # order the result groups by 1) the length of aggregatee list; 2) the number of their detected aggregations
                # the higher the group is in the rank, the more detected aggregations in a group and the longer the aggregatee list is
                result_grp_by_row_sign_operator = {k: v for k, v in
                                                   sorted(result_grp_by_row_sign_operator.items(), key=lambda e: (len(e[0][0][1]), len(e[1])), reverse=True)}
                filtered_results_column_wise = self.__filter_aggregations_by_signature(result_grp_by_row_sign_operator)
                filtered_results.extend(filtered_results_column_wise)

                file_aggrdet_results[number_format] = filtered_results
                pass
        pass

    def __filter_aggregations_by_signature(self, results_by_signature):
        # add a mark to the key. It indicates if this entry should be filtered out. A "1" means should be filtered out. All marks are initialized as "0"
        marks = [0 for _ in range(len(results_by_signature))]
        signatures = list(results_by_signature.keys())
        # aggregation_list = list(marked_results_by_signature.values())
        for index, (signature, aggregations) in enumerate(results_by_signature.items()):
            # signature is a 2er-tuple: (Signature, mark)

            # if mark is "1", skip this entry
            if marks[index] == 1:
                continue

            this_aggor = signature[0][0]
            this_aggees = signature[0][1]
            this_operator = signature[1]

            # set the marks of those entries after this, which should be filtered out, as "1"
            if all([e == 1 for e in marks[index + 1: len(results_by_signature)]]):
                break
            for that_index in range(index + 1, len(results_by_signature)):
                that_signature = signatures[that_index]
                if marks[that_index] == 1:
                    continue
                that_aggor = that_signature[0][0]
                that_aggees = that_signature[0][1]
                that_operator = that_signature[1]

                # if complete inclusion is satisfied (either way), filter out
                if this_aggor in that_aggees and len(set(this_aggees).intersection(set(that_aggees))) > 0:
                    marks[that_index] = 1
                if that_aggor in this_aggees and len(set(this_aggees).intersection(set(that_aggees))) > 0:
                    marks[that_index] = 1

                if this_operator == AggregationOperator.DIVISION.value or that_operator == AggregationOperator.DIVISION.value:
                    continue

                # if same aggregator, inclusive aggregatees happens (either way), filter out
                if this_aggor == that_aggor and len(set(this_aggees).intersection(set(that_aggees))) > 0:
                    marks[that_index] = 1

                # if circular aggregator-aggregatee happens (either way), filter out
                if this_aggor in that_aggees and that_aggor in this_aggees:
                    marks[that_index] = 1
        preserved_signatures = [signature for index, signature in enumerate(signatures) if marks[index] == 0]
        preserved_aggregations = [v for k, v in results_by_signature.items() if k in preserved_signatures]
        return list(itertools.chain(*preserved_aggregations))