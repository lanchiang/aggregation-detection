# Created by lan at 2021/4/13
import ast
import itertools
import json
import os
import time
from concurrent.futures import TimeoutError
from copy import copy, deepcopy
from decimal import Decimal

import luigi
import numpy as np
from luigi.mock import MockTarget
from pebble import ProcessPool
from tqdm import tqdm

from approach.aggrdet._AverageDetection import AverageDetection
from approach.aggrdet._SumDetection import SumDetection
from helpers import AggregationOperator, AggregationDirection
from number import get_indices_number_cells


class DelayedBruteforce(luigi.Task):
    dataset_path = luigi.Parameter()
    result_path = luigi.Parameter(default='./debug/')
    error_level = luigi.FloatParameter(default=0)
    target_aggregation_type = luigi.Parameter(default='All')
    error_strategy = luigi.Parameter(default='ratio')
    use_extend_strategy = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    timeout = luigi.FloatParameter(default=300)
    cpu_count = luigi.IntParameter(default=int(os.cpu_count() * 0.5))

    debug = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)

    NUMERIC_SATISFIED_RATIO = 0.5

    def output(self):
        if self.debug:
            return luigi.LocalTarget(os.path.join(self.result_path, 'delayed-bruteforce.jl'))
        else:
            return MockTarget('delayed-bruteforce')

    def requires(self):
        sum_detector = {'sum_detector': SumDetection(dataset_path=self.dataset_path, result_path=self.result_path, error_level=self.error_level,
                                                     use_extend_strategy=self.use_extend_strategy,
                                                     use_delayed_bruteforce=False, timeout=self.timeout, debug=self.debug)}
        average_detector = {'average_detector': AverageDetection(dataset_path=self.dataset_path, result_path=self.result_path, error_level=self.error_level,
                                                                 use_extend_strategy=self.use_extend_strategy,
                                                                 use_delayed_bruteforce=False, timeout=self.timeout, debug=self.debug)}
        all_detectors = {**sum_detector, **average_detector}

        required = {AggregationOperator.SUM.value: sum_detector,
                    AggregationOperator.AVERAGE.value: average_detector,
                    'All': all_detectors}.get(self.target_aggregation_type, None)

        if required is None:
            raise RuntimeError('Given target aggregation type parameter is illegal.')

        return required

    def run(self):
        gathered_detection_results = {}
        for key, _ in self.input().items():
            with self.input()[key].open('r') as file_reader:
                file_dicts = [json.loads(line) for line in file_reader]
                for file_dict in file_dicts:
                    file_signature = (file_dict['file_name'], file_dict['table_id'])
                    if file_signature not in gathered_detection_results:
                        gathered_detection_results[file_signature] = file_dict
                    else:
                        gathered = gathered_detection_results[file_signature]['aggregation_detection_result']
                        for number_format in gathered.keys():
                            if number_format in gathered:
                                gathered[number_format].extend(file_dict['aggregation_detection_result'][number_format])
                            else:
                                gathered[number_format] = file_dict['aggregation_detection_result'][number_format]

        # dict of file dicts, each file dict contains detection results of all specified aggregation type
        gathered_detection_results = list(gathered_detection_results.values())
        for gdr in gathered_detection_results:
            gdr['exec_time'][self.__class__.__name__] = -1000
            gdr['parameters']['use_delayed_bruteforce_strategy'] = True
        gathered_detection_result_maps = {(result['file_name'], result['table_id']): result for result in gathered_detection_results}
        print('Gathered detection results...')

        with ProcessPool(max_workers=self.cpu_count, max_tasks=1) as process_pool:
            delayed_bf_detection_results = process_pool.map(self.delayed_bruteforce_detection, gathered_detection_results, timeout=self.timeout).result()

        self.process_results(delayed_bf_detection_results, gathered_detection_result_maps, self.__class__.__name__)

        with self.output().open('w') as file_writer:
            for file_output_dict in tqdm(gathered_detection_result_maps.values(), desc='Serialize results'):
                file_writer.write(json.dumps(file_output_dict) + '\n')

    def delayed_bruteforce_detection_rowwise(self, _file_dict):
        file_dict = deepcopy(_file_dict)
        start_time = time.time()

        current_detected_aggregations = file_dict['aggregation_detection_result']

        for valid_nf, transformed_content in file_dict['valid_number_formats'].items():
            # Todo: compromise, no use only the correct number format
            if valid_nf != file_dict['number_format']:
                continue

            # Todo: wrap the file dict with a class, so that accessing an entry of the dict is actually calling a function.

            # Todo: have I not already implemented a number checking function? Should reuse it.
            numberized_indices = get_indices_number_cells(transformed_content)
            numberized_values_indexed = {
                index: (Decimal(0.0) if Decimal(transformed_content[index[0]][index[1]]).is_nan() else Decimal(transformed_content[index[0]][index[1]])) for
                index in numberized_indices}
            numberized_indices_grpby_column = [list(it) for k, it in itertools.groupby(sorted(numberized_indices, key=lambda x: x[1]), lambda x: x[1])]
            file_length = file_dict['num_rows']
            numberized_indices_grpby_column = {e[0][1]: e for e in numberized_indices_grpby_column if len(e) / file_length >= self.NUMERIC_SATISFIED_RATIO}

            numeric_row_indices = [list(it)[0][0] for k, it in itertools.groupby(sorted(numberized_indices, key=lambda x: x[0]), lambda x: x[0])]

            # construct filtering rule-sets
            # row wise
            filtering_ruleset_rowwise = {operator: {k: set() for k in numberized_indices_grpby_column.keys()} for operator in AggregationOperator.all()}
            for cda in current_detected_aggregations[valid_nf]:
                if cda[3] != AggregationDirection.ROW_WISE.value:
                    continue
                aggregator_index = ast.literal_eval(cda[0])
                aggregatee_indices = [ast.literal_eval(e) for e in cda[1]]
                operator = cda[2]
                for ae_i in aggregatee_indices:
                    for o in AggregationOperator.all():
                        if ae_i[1] not in filtering_ruleset_rowwise[o]:
                            filtering_ruleset_rowwise[o][ae_i[1]] = set()
                        filtering_ruleset_rowwise[o][ae_i[1]].add(aggregator_index[1])
                        chained_lhss = [lhs for v in filtering_ruleset_rowwise.values() for lhs, rhs in v.items() if ae_i[1] in rhs]
                        for chained_lhs in chained_lhss:
                            filtering_ruleset_rowwise[o][chained_lhs].add(aggregator_index[1])

                        # an aggregatee cell cannot be an aggregator of its siblings aggregatees
                        for siblings in [e for e in aggregatee_indices if e != ae_i]:
                            filtering_ruleset_rowwise[o][ae_i[1]].add(siblings[1])
                # if aggregator_index[1] < aggregatee_indices[0][1]:
                #     for ci in numberized_indices_grpby_column.keys():
                #         if ci < aggregator_index[1]:
                #             filtering_ruleset_rowwise[operator][aggregator_index[1]].add(ci)
                # else:
                #     for ci in numberized_indices_grpby_column.keys():
                #         if ci > aggregator_index[1]:
                #             filtering_ruleset_rowwise[operator][aggregator_index[1]].add(ci)

            results = []

            for i in numberized_indices_grpby_column.keys():
                conflict_column_indices = filtering_ruleset_rowwise.get(i, [])
                all_valid_aggre_column_indices = [ci for ci in numberized_indices_grpby_column.keys() if ci != i and ci not in conflict_column_indices]
                for column_combination_size in reversed(range(2, len(all_valid_aggre_column_indices) + 1)):
                    for column_combination in itertools.combinations(all_valid_aggre_column_indices, column_combination_size):
                        # if not (i == 2 and column_combination == (5, 8, 11, 14, 17)):
                        #     continue
                        # Todo: REMEMBER TO CHANGE THIS HARD CODE HERE.
                        for aggregation_type in [AggregationOperator.SUM.value, AggregationOperator.AVERAGE.value]:
                            rhs = set.union(*[filtering_ruleset_rowwise[aggregation_type][c] for c in column_combination])
                            if bool(set.intersection(set(column_combination), rhs)):
                                continue
                            candidate_aggr_indices = [((row_index, i), [(row_index, ae_column_index) for ae_column_index in column_combination]) for row_index
                                                      in numeric_row_indices]
                            candidate_results = [self.is_aggregation(cai[0], cai[1], numberized_values_indexed, self.error_level, aggregation_type, 0) for cai
                                                 in candidate_aggr_indices]
                            satisfied_results = [r for r in candidate_results if r is not None]
                            if len(satisfied_results) / len(candidate_aggr_indices) >= self.NUMERIC_SATISFIED_RATIO:
                                results.extend(satisfied_results)
                                for filtering_rules in filtering_ruleset_rowwise.values():
                                    for c in column_combination:
                                        filtering_rules[c].add(i)
                        pass

            file_dict['aggregation_detection_result'][valid_nf] = results
        end_time = time.time()
        exec_time = end_time - start_time
        file_dict['exec_time']['RowWiseDelayedBruteforce'] = exec_time
        return file_dict

    def delayed_bruteforce_detection_columnwise(self, _file_dict):
        file_dict = deepcopy(_file_dict)
        start_time = time.time()

        current_detected_aggregations = file_dict['aggregation_detection_result']

        for valid_nf, transformed_content in file_dict['valid_number_formats'].items():
            # Todo: compromise, no use only the correct number format
            if valid_nf != file_dict['number_format']:
                continue

            numberized_indices = get_indices_number_cells(transformed_content)
            numberized_values_indexed = {
                index: (Decimal(0.0) if Decimal(transformed_content[index[0]][index[1]]).is_nan() else Decimal(transformed_content[index[0]][index[1]])) for
                index in numberized_indices}
            numberized_indices_grpby_row = [list(it) for k, it in itertools.groupby(sorted(numberized_indices, key=lambda x: x[0]), lambda x: x[0])]
            file_width = file_dict['num_cols']
            numberized_indices_grpby_row = {e[0][0]: e for e in numberized_indices_grpby_row if len(e) / file_width >= self.NUMERIC_SATISFIED_RATIO}

            numeric_column_indices = [list(it)[0][1] for k, it in itertools.groupby(sorted(numberized_indices, key=lambda x: x[1]), lambda x: x[1])]

            # construct filtering rule-sets
            # column wise
            filtering_ruleset_columnwise = {operator: {k: set() for k in numberized_indices_grpby_row.keys()} for operator in AggregationOperator.all()}
            for cda in current_detected_aggregations[valid_nf]:
                if cda[3] != AggregationDirection.COLUMN_WISE.value:
                    continue
                aggregator_index = ast.literal_eval(cda[0])
                aggregatee_indices = [ast.literal_eval(e) for e in cda[1]]
                operator = cda[2]
                for ae_i in aggregatee_indices:
                    for o in AggregationOperator.all():
                        if ae_i[0] not in filtering_ruleset_columnwise[o]:
                            filtering_ruleset_columnwise[o][ae_i[0]] = set()
                        filtering_ruleset_columnwise[o][ae_i[0]].add(aggregator_index[0])
                        chained_lhss = [lhs for v in filtering_ruleset_columnwise.values() for lhs, rhs in v.items() if ae_i[0] in rhs]
                        for chained_lhs in chained_lhss:
                            filtering_ruleset_columnwise[o][chained_lhs].add(aggregator_index[0])

                        # an aggregatee cell cannot be an aggregator of its siblings aggregatees
                        for siblings in [e for e in aggregatee_indices if e != ae_i]:
                            filtering_ruleset_columnwise[o][ae_i[0]].add(siblings[0])
                # if aggregator_index[0] < aggregatee_indices[0][0]:
                #     for ci in numberized_indices_grpby_row.keys():
                #         if ci < aggregator_index[0]:
                #             filtering_ruleset_columnwise[operator][aggregator_index[0]].add(ci)
                # else:
                #     for ci in numberized_indices_grpby_row.keys():
                #         if ci > aggregator_index[0]:
                #             filtering_ruleset_columnwise[operator][aggregator_index[0]].add(ci)

            results = []

            for i in numberized_indices_grpby_row.keys():
                conflict_row_indices = filtering_ruleset_columnwise.get(i, [])
                all_valid_aggre_row_indices = [ci for ci in numberized_indices_grpby_row.keys() if ci != i and ci not in conflict_row_indices]
                for row_combination_size in reversed(range(2, len(all_valid_aggre_row_indices) + 1)):
                    for row_combination in itertools.combinations(all_valid_aggre_row_indices, row_combination_size):
                        # print((i, row_combination))
                        # Todo: REMEMBER TO CHANGE THIS HARD CODE HERE.
                        for aggregation_type in [AggregationOperator.SUM.value, AggregationOperator.AVERAGE.value]:
                            rhs = set.union(*[filtering_ruleset_columnwise[aggregation_type][c] for c in row_combination])
                            if bool(set.intersection(set(row_combination), rhs)):
                                continue
                            candidate_aggr_indices = [((i, column_index), [(ae_row_index, column_index) for ae_row_index in row_combination]) for column_index
                                                      in numeric_column_indices]
                            candidate_results = [self.is_aggregation(cai[0], cai[1], numberized_values_indexed, self.error_level, aggregation_type, 1) for cai
                                                 in
                                                 candidate_aggr_indices]
                            satisfied_results = [r for r in candidate_results if r is not None]
                            if len(satisfied_results) / len(candidate_aggr_indices) >= self.NUMERIC_SATISFIED_RATIO:
                                results.extend(satisfied_results)
                                for filtering_rules in filtering_ruleset_columnwise.values():
                                    for c in row_combination:
                                        filtering_rules[c].add(i)
                        pass

            file_dict['aggregation_detection_result'][valid_nf] = results
        end_time = time.time()
        exec_time = end_time - start_time
        file_dict['exec_time']['ColumnWiseDelayedBruteforce'] = exec_time
        return file_dict

    def delayed_bruteforce_detection(self, file_dict):
        # print(file_dict['file_name'])
        rowwise_results = self.delayed_bruteforce_detection_rowwise(file_dict)
        columnwise_results = self.delayed_bruteforce_detection_columnwise(file_dict)
        return rowwise_results, columnwise_results

    def is_aggregation(self, aggregator, aggregatees, numberized_values_indexed, error_level, operator, axis):
        is_aggr = False
        # if the cell contains no numeric values, return None
        try:
            aggregator_value = numberized_values_indexed[aggregator]
            aggregatee_values = [numberized_values_indexed[e] for e in aggregatees if e in numberized_values_indexed]
        except KeyError:
            return None
        # aggregatee cardinality smaller than 2 are not valid in our definition
        if len(aggregatee_values) <= 1:
            return None

        if operator == AggregationOperator.SUM.value:
            if aggregator_value == 0.0:
                if abs(sum(aggregatee_values) - aggregator_value) <= error_level:
                    is_aggr = True
            else:
                if abs((sum(aggregatee_values) - aggregator_value) / aggregator_value) <= error_level:
                    is_aggr = True
        elif operator == AggregationOperator.AVERAGE.value:
            if aggregator_value == 0.0:
                if abs(round(sum(aggregatee_values) / len(aggregatee_values), 5) - aggregator_value) <= error_level:
                    is_aggr = True
            else:
                if abs((round(sum(aggregatee_values) / len(aggregatee_values), 5) - aggregator_value) / aggregator_value) <= error_level:
                    is_aggr = True

        result = None
        if is_aggr:
            aggregator_index = str(aggregator)
            aggregatee_indices = [str(e) for e in aggregatees]
            result = (
                aggregator_index, aggregatee_indices, operator, AggregationDirection.ROW_WISE.value if axis == 0 else AggregationDirection.COLUMN_WISE.value)
        return result

    def process_results(self, results, files_dict_map, task_name):
        while True:
            try:
                result = next(results)
            except StopIteration:
                break
            except TimeoutError:
                pass
            except ValueError:
                continue
            else:
                row_wise_result = result[0]
                column_wise_result = result[1]
                file_name = row_wise_result['file_name']
                sheet_name = row_wise_result['table_id']
                merged_result = copy(row_wise_result)
                for nf in merged_result['aggregation_detection_result'].keys():
                    merged_result['aggregation_detection_result'][nf] = files_dict_map[(file_name, sheet_name)]['aggregation_detection_result'][nf] + \
                                                                        merged_result['aggregation_detection_result'][nf] + \
                                                                        column_wise_result['aggregation_detection_result'][nf]
                    # merged_result['aggregation_detection_result'][nf].extend(column_wise_result['aggregation_detection_result'][nf])
                merged_result['exec_time'][task_name] = \
                    merged_result['exec_time']['RowWiseDetection'] + merged_result['exec_time']['ColumnWiseDetection']
                files_dict_map[(file_name, sheet_name)] = merged_result
