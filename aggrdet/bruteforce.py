# Created by lan at 2021/3/2
import ast
import decimal
import itertools
import json
import os
import time
from concurrent.futures import TimeoutError
from decimal import Decimal

import luigi
import numpy as np
from luigi.mock import MockTarget
from pebble import ProcessPool
from tqdm import tqdm

from dataprep import NumberFormatNormalization
from helpers import AggregationOperator
from number import get_indices_number_cells


class Baseline(luigi.Task):
    dataset_path = luigi.Parameter()
    result_path = luigi.Parameter()
    error_level = luigi.FloatParameter(default=0)
    timeout = luigi.FloatParameter(default=300)
    verbose = luigi.BoolParameter(default=True, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    debug = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)

    def output(self):
        if self.debug:
            return luigi.LocalTarget(os.path.join(self.result_path, 'baseline.jl'))
        else:
            return MockTarget('baseline')

    def requires(self):
        return NumberFormatNormalization(self.dataset_path, result_path=self.result_path, debug=self.debug)

    def run(self):
        with self.input().open('r') as input_file:
            files_dict = [json.loads(line) for line in input_file]
            files_dict_map = {}
            for file_dict in files_dict:
                file_dict['detected_number_format'] = ''
                file_dict['detected_aggregations'] = []
                file_dict['exec_time'][self.__class__.__name__] = -1000
                file_dict['parameters'] = {}
                file_dict['parameters']['error_level'] = self.error_level
                file_dict['parameters']['error_strategy'] = None
                file_dict['parameters']['use_extend_strategy'] = None
                file_dict['parameters']['use_delayed_bruteforce_strategy'] = None
                file_dict['parameters']['timeout'] = self.timeout
                file_dict['parameters']['algorithm'] = self.__class__.__name__

                files_dict_map[(file_dict['file_name'], file_dict['table_id'])] = file_dict

        cpu_count = int(os.cpu_count() * 0.75)

        with ProcessPool(max_workers=cpu_count, max_tasks=1) as pool:
            returned_result = pool.map(self.detect_aggregations, files_dict, timeout=self.timeout).result()

        print('Detection process is done. Now write results to file.')

        while True:
            try:
                result = next(returned_result)
            except StopIteration:
                break
            except TimeoutError:
                pass
            else:
                file_name = result['file_name']
                sheet_name = result['table_id']
                files_dict_map[(file_name, sheet_name)] = result

        with self.output().open('w') as file_writer:
            for file_output_dict in tqdm(files_dict_map.values(), desc='Serialize results'):
                file_writer.write(json.dumps(file_output_dict) + '\n')

    def detect_aggregations(self, file_json_dict):
        start_time = time.time()

        results_per_nf = {}
        for valid_nf, transformed_content in file_json_dict['valid_number_formats'].items():
            if valid_nf != file_json_dict['number_format']:
                continue
            numberized_indices = get_indices_number_cells(transformed_content)
            results = []
            indexed_values = [(index, value) for index, value in np.ndenumerate(transformed_content)]
            for index, value in tqdm(indexed_values, desc=file_json_dict['file_name']):
                if index not in numberized_indices:
                    continue
                numberized_value = Decimal(value)
                aggregator = numberized_value
                # check the same row
                row_index = index[0]
                numbers_same_row = [(elem, Decimal(transformed_content[elem[0]][elem[1]])) for elem in numberized_indices
                                    if elem[0] == row_index and elem != index]
                for i in range(1, len(numbers_same_row)):
                    results.extend(list(itertools.chain(*[is_aggregation((index, aggregator), aggregatees, self.error_level)
                                                          for aggregatees in itertools.combinations(numbers_same_row, i + 1)])))
                # check the same column
                column_index = index[1]
                numbers_same_column = [(elem, Decimal(transformed_content[elem[0]][elem[1]])) for elem in numberized_indices
                                       if elem[1] == column_index and elem != index]
                for i in range(1, len(numbers_same_column)):
                    results.extend(list(itertools.chain(*[is_aggregation((index, aggregator), aggee_comb, self.error_level)
                                                          for aggee_comb in itertools.combinations(numbers_same_column, i + 1)])))
            results_per_nf[valid_nf] = results
        if results_per_nf == {}:
            selected_nf = ""
            detected_aggrs = []
        else:
            selected_nf = max(results_per_nf, key=lambda k: len(results_per_nf[k]))
            detected_aggrs = results_per_nf[selected_nf]

        end_time = time.time()
        exec_time = end_time - start_time
        file_json_dict['detected_number_format'] = selected_nf
        detected_aggrs = [{'aggregator_index': ast.literal_eval(detected_aggregation[0]), 'aggregatee_indices': [ast.literal_eval(e) for e in detected_aggregation[1]]}
                          for detected_aggregation in detected_aggrs]
        file_json_dict['detected_aggregations'] = detected_aggrs
        file_json_dict['exec_time'][self.__class__.__name__] = exec_time

        return file_json_dict


def is_aggregation(aggregator, aggregatees, error_level):
    aggregation_results = []
    # sum?
    is_sum = False
    if aggregator[1] == 0.0:
        if abs(sum([e[1] for e in aggregatees]) - aggregator[1]) <= error_level:
            is_sum = True
    else:
        if abs((sum([e[1] for e in aggregatees]) - aggregator[1]) / aggregator[1]) <= error_level:
            is_sum = True
        # try:
        #     if abs((sum([e[1] for e in aggregatees]) - aggregator[1]) / aggregator[1]) <= error_level:
        #         is_sum = True
        # except decimal.InvalidOperation as ioe:
        #     print(aggregator)
        #     print(aggregatees)
    if is_sum:
        aggregator_index = aggregator[0]
        aggregatee_indices = [str(e[0]) for e in aggregatees]
        aggregation_results.append((str(aggregator_index), aggregatee_indices, 'Sum'))
        # aggregation_results.append({'aggregator_index': aggregator_index, 'aggregatee_indices': aggregatee_indices, 'operator': 'Sum'})
        # aggregation_results.append((aggregator_index, aggregatee_indices, 'Sum'))
    return aggregation_results


def filter_conflict_bruteforce_results(raw_bruteforce_results, aggregations, axis=0):
    if axis == 0:
        # row-wise
        survived_results = []
        for raw_result in raw_bruteforce_results:
            if raw_result[2] != AggregationOperator.SUM.value:
                continue
            result_aggregator_column_index = ast.literal_eval(raw_result[0])[1]
            result_aggregatee_column_indices = [ast.literal_eval(result_aggregatee)[1] for result_aggregatee in raw_result[1]]
            one_side = [result_aggregator_column_index < result_aggregatee_column_index for result_aggregatee_column_index in result_aggregatee_column_indices]
            if not all(one_side) and any(one_side):
                continue
            else:
                survived_results.append(raw_result)

        # conflict rule 1: bidirectional aggregation
        bidirectional_survived_results = []
        for survived_result in survived_results:
            survived_aggregator_column_index = ast.literal_eval(survived_result[0])[1]
            survived_aggregatee_column_indices = [ast.literal_eval(result_aggregatee)[1] for result_aggregatee in survived_result[1]]
            survived_aggregate_left = all([ci < survived_aggregator_column_index for ci in survived_aggregatee_column_indices])
            bidirectional_candidates = [aggregation for aggregation in aggregations if ast.literal_eval(aggregation[0])[1] == survived_aggregator_column_index]
            can_survive = True
            for aggregation in bidirectional_candidates:
                if not (survived_aggregate_left == all([ast.literal_eval(aggregatee)[1] < ast.literal_eval(aggregation[0])[1] for aggregatee in aggregation[1]])):
                    can_survive = False
                    break
            if can_survive:
                bidirectional_survived_results.append(survived_result)

        # conflict rule 2: complete inclusion
        complete_inclusion_survived_results = []
        for survived_result in bidirectional_survived_results:
            survived_aggregator_column_index = ast.literal_eval(survived_result[0])[1]
            survived_aggregatee_column_indices = [ast.literal_eval(result_aggregatee)[1] for result_aggregatee in survived_result[1]]
            can_survive = True
            for aggregation in aggregations:
                aggregator_column_index = ast.literal_eval(aggregation[0])[1]
                aggregatee_column_indices = [ast.literal_eval(aggregatee)[1] for aggregatee in aggregation[1]]
                aggee_overlap = list(set(survived_aggregatee_column_indices) & set(aggregatee_column_indices))
                if survived_aggregator_column_index in aggregatee_column_indices and bool(aggee_overlap):
                    can_survive = False
                    break
                if aggregator_column_index in survived_aggregatee_column_indices and bool(aggee_overlap):
                    can_survive = False
                    break
            if can_survive:
                complete_inclusion_survived_results.append(survived_result)

        # conflict rule 3: partial aggregatees overlap
        partial_aggee_overlap_survived_results = []
        for survived_result in complete_inclusion_survived_results:
            survived_aggregatee_column_indices = [ast.literal_eval(result_aggregatee)[1] for result_aggregatee in survived_result[1]]
            can_survive = True
            for aggregation in aggregations:
                aggregatee_column_indices = [ast.literal_eval(aggregatee)[1] for aggregatee in aggregation[1]]
                ar_aggee_set = set(aggregatee_column_indices)
                survived_aggee_set = set(survived_aggregatee_column_indices)
                aggee_overlap = list(ar_aggee_set & survived_aggee_set)
                if not (len(aggee_overlap) == 0 or (len(aggee_overlap) == len(ar_aggee_set) and len(aggee_overlap) == len(survived_aggee_set))):
                    can_survive = False
                    break
            if can_survive:
                partial_aggee_overlap_survived_results.append(survived_result)
        filtered_results = partial_aggee_overlap_survived_results
    else:
        # column-wise
        survived_results = []
        for raw_result in raw_bruteforce_results:
            if raw_result[2] != AggregationOperator.SUM.value:
                continue
            result_aggregator_row_index = ast.literal_eval(raw_result[0])[0]
            result_aggregatee_row_indices = [ast.literal_eval(result_aggregatee)[0] for result_aggregatee in raw_result[1]]
            one_side = [result_aggregator_row_index < result_aggregatee_row_index for result_aggregatee_row_index in result_aggregatee_row_indices]
            if not all(one_side) and any(one_side):
                continue
            else:
                survived_results.append(raw_result)

        # conflict rule 1: bidirectional aggregation
        bidirectional_survived_results = []
        for survived_result in survived_results:
            survived_aggregator_row_index = ast.literal_eval(survived_result[0])[0]
            survived_aggregatee_row_indices = [ast.literal_eval(result_aggregatee)[0] for result_aggregatee in survived_result[1]]
            survived_aggregate_left = all([ci < survived_aggregator_row_index for ci in survived_aggregatee_row_indices])
            bidirectional_candidates = [aggregation for aggregation in aggregations if ast.literal_eval(aggregation[0])[0] == survived_aggregator_row_index]
            can_survive = True
            for aggregation in bidirectional_candidates:
                if not (survived_aggregate_left == all([ast.literal_eval(aggregatee)[0] < ast.literal_eval(aggregation[0])[0] for aggregatee in aggregation[1]])):
                    can_survive = False
                    break
            if can_survive:
                bidirectional_survived_results.append(survived_result)

        # conflict rule 2: complete inclusion
        complete_inclusion_survived_results = []
        for survived_result in bidirectional_survived_results:
            survived_aggregator_row_index = ast.literal_eval(survived_result[0])[0]
            survived_aggregatee_row_indices = [ast.literal_eval(result_aggregatee)[0] for result_aggregatee in survived_result[1]]
            can_survive = True
            for aggregation in aggregations:
                aggregator_row_index = ast.literal_eval(aggregation[0])[0]
                aggregatee_row_indices = [ast.literal_eval(aggregatee)[0] for aggregatee in aggregation[1]]
                aggee_overlap = list(set(survived_aggregatee_row_indices) & set(aggregatee_row_indices))
                if survived_aggregator_row_index in aggregatee_row_indices and bool(aggee_overlap):
                    can_survive = False
                    break
                if aggregator_row_index in survived_aggregatee_row_indices and bool(aggee_overlap):
                    can_survive = False
                    break
            if can_survive:
                complete_inclusion_survived_results.append(survived_result)

        # conflict rule 3: partial aggregatees overlap
        partial_aggee_overlap_survived_results = []
        for survived_result in complete_inclusion_survived_results:
            survived_aggregatee_row_indices = [ast.literal_eval(result_aggregatee)[0] for result_aggregatee in survived_result[1]]
            can_survive = True
            for aggregation in aggregations:
                aggregatee_row_indices = [ast.literal_eval(aggregatee)[0] for aggregatee in aggregation[1]]
                ar_aggee_set = set(aggregatee_row_indices)
                survived_aggee_set = set(survived_aggregatee_row_indices)
                aggee_overlap = list(ar_aggee_set & survived_aggee_set)
                if not (len(aggee_overlap) == 0 or (len(aggee_overlap) == len(ar_aggee_set) and len(aggee_overlap) == len(survived_aggee_set))):
                    can_survive = False
                    break
            if can_survive:
                partial_aggee_overlap_survived_results.append(survived_result)
        filtered_results = partial_aggee_overlap_survived_results
    return filtered_results


def delayed_bruteforce(collected_results_by_line, file_arr, error_level, axis):
    file_array = np.array(file_arr)
    number_indices = get_indices_number_cells(file_array)
    if axis == 0:
        # row-wise
        for row_index, (key, aggregations) in enumerate(collected_results_by_line.items()):
            impossible_ar_partials = {}
            for aggregation in aggregations:
                aggregator = ast.literal_eval(aggregation[0])
                aggregatees = [ast.literal_eval(ae) for ae in aggregation[1]]
                for ae in aggregatees:
                    if ae[1] not in impossible_ar_partials:
                        impossible_ar_partials[ae[1]] = []
                    impossible_ar_partials[ae[1]].append(aggregator[1])
                    chained_lhss = [lhs for lhs, rhs in impossible_ar_partials.items() if ae[1] in rhs]
                    for chained_lhs in chained_lhss:
                        impossible_ar_partials[chained_lhs].append(aggregator[1])

            for index, value in np.ndenumerate(file_array[row_index]):
                cell_index = (row_index, index[0])
                if cell_index not in number_indices:
                    continue
                numberized_value = Decimal(value)
                if numberized_value.is_nan():
                    aggregator = Decimal(0.0)
                else:
                    aggregator = numberized_value
                impossibles = impossible_ar_partials.get(cell_index[1], [])
                if bool(impossibles):
                    numbers_same_row = []
                    for elem in number_indices:
                        if elem[0] == cell_index[0] and elem != cell_index and elem[1] not in impossibles:
                            number_same_row = Decimal(file_array[elem[0]][elem[1]])
                            number_same_row = number_same_row if not number_same_row.is_nan() else Decimal(0.0)
                            numbers_same_row.append((elem, number_same_row))
                    # numbers_same_row = [(elem, Decimal(file_array[elem[0]][elem[1]])) for elem in number_indices
                    #                     if elem[0] == cell_index[0] and elem != cell_index and elem[1] not in impossibles]
                else:
                    numbers_same_row = []
                    for elem in number_indices:
                        if elem[0] == cell_index[0] and elem != cell_index:
                            number_same_row = Decimal(file_array[elem[0]][elem[1]])
                            number_same_row = number_same_row if not number_same_row.is_nan() else Decimal(0.0)
                            numbers_same_row.append((elem, number_same_row))
                    # numbers_same_row = [(elem, Decimal(file_array[elem[0]][elem[1]])) for elem in number_indices
                    #                     if elem[0] == cell_index[0] and elem != cell_index]
                for i in range(1, len(numbers_same_row)):
                    raw_bruteforce_results = itertools.chain(*[is_aggregation((cell_index, aggregator), aggregatees, error_level)
                                                                                 for aggregatees in itertools.combinations(numbers_same_row, i + 1)])
                    filtered_results = filter_conflict_bruteforce_results(raw_bruteforce_results, aggregations, axis=0)
                    collected_results_by_line[key].extend(filtered_results)
    elif axis == 1:
        # column-wise
        for column_index, (key, aggregations) in enumerate(collected_results_by_line.items()):
            impossible_ar_partials = {}
            for aggregation in aggregations:
                aggregator = ast.literal_eval(aggregation[0])
                aggregatees = [ast.literal_eval(ae) for ae in aggregation[1]]
                for ae in aggregatees:
                    if ae[0] not in impossible_ar_partials:
                        impossible_ar_partials[ae[0]] = []
                    impossible_ar_partials[ae[0]].append(aggregator[0])
                    chained_lhss = [lhs for lhs, rhs in impossible_ar_partials.items() if ae[0] in rhs]
                    for chained_lhs in chained_lhss:
                        impossible_ar_partials[chained_lhs].append(aggregator[0])

            for index, value in np.ndenumerate(file_array[:, column_index]):
                cell_index = (index[0], column_index)
                if cell_index not in number_indices:
                    continue
                numberized_value = Decimal(value)
                if numberized_value.is_nan():
                    aggregator = Decimal(0.0)
                else:
                    aggregator = numberized_value
                # aggregator = numberized_value
                impossibles = impossible_ar_partials.get(cell_index[0], [])
                if bool(impossibles):
                    numbers_same_column = []
                    for elem in number_indices:
                        if elem[1] == cell_index[1] and elem != cell_index and elem[0] not in impossibles:
                            number_same_column = Decimal(file_array[elem[0]][elem[1]])
                            number_same_column = number_same_column if not number_same_column.is_nan() else Decimal(0.0)
                            numbers_same_column.append((elem, number_same_column))
                    # numbers_same_column = [(elem, Decimal(file_array[elem[0]][elem[1]])) for elem in number_indices
                    #                         if elem[1] == cell_index[1] and elem != cell_index and elem[0] not in impossibles]
                else:
                    numbers_same_column = []
                    for elem in number_indices:
                        if elem[1] == cell_index[1] and elem != cell_index:
                            number_same_column = Decimal(file_array[elem[0]][elem[1]])
                            number_same_column = number_same_column if not number_same_column.is_nan() else Decimal(0.0)
                            numbers_same_column.append((elem, number_same_column))
                    # numbers_same_column = [(elem, Decimal(file_array[elem[0]][elem[1]])) for elem in number_indices
                    #                        if elem[1] == cell_index[1] and elem != cell_index]
                for i in range(1, len(numbers_same_column)):
                    raw_bruteforce_results = itertools.chain(*[is_aggregation((cell_index, aggregator), aggregatees, error_level)
                                                                                 for aggregatees in itertools.combinations(numbers_same_column, i + 1)])
                    filtered_results = filter_conflict_bruteforce_results(raw_bruteforce_results, aggregations, axis=1)
                    collected_results_by_line[key].extend(filtered_results)
    else:
        raise RuntimeError('Illegal axis parameter for brute force approach.')


if __name__ == '__main__':
    luigi.run()
