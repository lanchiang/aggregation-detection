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

# from algorithm import NumberFormatNormalization
from number import get_indices_number_cells


class Baseline(luigi.Task):
    dataset_path = luigi.Parameter()
    error_level = luigi.FloatParameter(default=0)
    timeout = luigi.FloatParameter(default=300)
    verbose = luigi.BoolParameter(default=True, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    #
    # def output(self):
    #     return MockTarget('baseline')
    #
    # def requires(self):
    #     return NumberFormatNormalization(self.dataset_path)
    #
    # def run(self):
    #     with self.input().open('r') as input_file:
    #         files_dict = [json.loads(line) for line in input_file]
    #         files_dict_map = {}
    #         for file_dict in files_dict:
    #             file_dict['detected_number_format'] = ''
    #             file_dict['detected_aggregations'] = []
    #             file_dict['exec_time'][self.__class__.__name__] = -1000
    #             file_dict['parameters'] = {}
    #             file_dict['parameters']['error_level'] = self.error_level
    #             file_dict['parameters']['error_strategy'] = None
    #             file_dict['parameters']['use_extend_strategy'] = None
    #             file_dict['parameters']['timeout'] = self.timeout
    #             file_dict['parameters']['algorithm'] = self.__class__.__name__
    #
    #             files_dict_map[(file_dict['file_name'], file_dict['table_id'])] = file_dict
    #
    #     cpu_count = int(os.cpu_count() / 2)
    #
    #     with ProcessPool(max_workers=cpu_count, max_tasks=1) as pool:
    #         returned_result = pool.map(self.detect_aggregations, files_dict, timeout=self.timeout).result()
    #
    #     print('Detection process is done. Now write results to file.')
    #
    #     while True:
    #         try:
    #             result = next(returned_result)
    #         except StopIteration:
    #             break
    #         except TimeoutError:
    #             pass
    #         else:
    #             file_name = result['file_name']
    #             sheet_name = result['table_id']
    #             files_dict_map[(file_name, sheet_name)] = result
    #             # print(file_name + '\t' + str(result['exec_time'][self.__class__.__name__]))
    #
    #     with self.output().open('w') as file_writer:
    #         for file_output_dict in tqdm(files_dict_map.values(), desc='Serialize results'):
    #             file_writer.write(json.dumps(file_output_dict) + '\n')
    #
    # def detect_aggregations(self, file_json_dict):
    #     start_time = time.time()
    #
    #     results_per_nf = {}
    #     for valid_nf, transformed_content in file_json_dict['valid_number_formats'].items():
    #         if valid_nf != file_json_dict['number_format']:
    #             continue
    #         # numberized_indices = set()
    #         # for index, value in np.ndenumerate(transformed_content):
    #         #     try:
    #         #         Decimal(value)
    #         #     except decimal.InvalidOperation:
    #         #         continue
    #         #     else:
    #         #         numberized_indices.add(index)
    #         numberized_indices = get_indices_number_cells(transformed_content)
    #         results = []
    #         indexed_values = [(index, value) for index, value in np.ndenumerate(transformed_content)]
    #         for index, value in tqdm(indexed_values, desc=file_json_dict['file_name']):
    #             if index not in numberized_indices:
    #                 continue
    #             numberized_value = Decimal(value)
    #             aggregator = numberized_value
    #             # check the same row
    #             row_index = index[0]
    #             numbers_same_row = [(elem, Decimal(transformed_content[elem[0]][elem[1]])) for elem in numberized_indices
    #                                 if elem[0] == row_index and elem != index]
    #             for i in range(1, len(numbers_same_row)):
    #                 results.extend(list(itertools.chain(*[is_aggregation((index, aggregator), aggregatees, self.error_level)
    #                                                       for aggregatees in itertools.combinations(numbers_same_row, i + 1)])))
    #             # check the same column
    #             column_index = index[1]
    #             numbers_same_column = [(elem, Decimal(transformed_content[elem[0]][elem[1]])) for elem in numberized_indices
    #                                    if elem[1] == column_index and elem != index]
    #             for i in range(1, len(numbers_same_column)):
    #                 results.extend(list(itertools.chain(*[is_aggregation((index, aggregator), aggee_comb, self.error_level)
    #                                                       for aggee_comb in itertools.combinations(numbers_same_column, i + 1)])))
    #         results_per_nf[valid_nf] = results
    #     if results_per_nf == {}:
    #         selected_nf = ""
    #         detected_aggrs = []
    #     else:
    #         selected_nf = max(results_per_nf, key=lambda k: len(results_per_nf[k]))
    #         detected_aggrs = results_per_nf[selected_nf]
    #
    #     end_time = time.time()
    #     exec_time = end_time - start_time
    #     file_json_dict['detected_number_format'] = selected_nf
    #     file_json_dict['detected_aggregations'] = detected_aggrs
    #     file_json_dict['exec_time'][self.__class__.__name__] = exec_time
    #
    #     return file_json_dict


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
    if is_sum:
        aggregator_index = aggregator[0]
        aggregatee_indices = [str(e[0]) for e in aggregatees]
        aggregation_results.append((str(aggregator_index), aggregatee_indices, 'Sum'))
        # aggregation_results.append({'aggregator_index': aggregator_index, 'aggregatee_indices': aggregatee_indices, 'operator': 'Sum'})
        # aggregation_results.append((aggregator_index, aggregatee_indices, 'Sum'))
    return aggregation_results


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
                aggregator = numberized_value
                impossibles = impossible_ar_partials.get(cell_index[1], [])
                if bool(impossibles):
                    numbers_same_row = [(elem, Decimal(file_array[elem[0]][elem[1]])) for elem in number_indices
                                        if elem[0] == cell_index[0] and elem != cell_index and elem[1] not in impossibles]
                else:
                    numbers_same_row = [(elem, Decimal(file_array[elem[0]][elem[1]])) for elem in number_indices
                                        if elem[0] == cell_index[0] and elem != cell_index]
                for i in range(1, len(numbers_same_row)):
                    collected_results_by_line[key].extend(list(itertools.chain(*[is_aggregation((cell_index, aggregator), aggregatees, error_level)
                                                                                 for aggregatees in itertools.combinations(numbers_same_row, i + 1)])))
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
                aggregator = numberized_value
                impossibles = impossible_ar_partials.get(cell_index[0], [])
                if bool(impossibles):
                    numbers_same_column = [(elem, Decimal(file_array[elem[0]][elem[1]])) for elem in number_indices
                                            if elem[1] == cell_index[1] and elem != cell_index and elem[0] not in impossibles]
                else:
                    numbers_same_column = [(elem, Decimal(file_array[elem[0]][elem[1]])) for elem in number_indices
                                           if elem[1] == cell_index[1] and elem != cell_index]
                for i in range(1, len(numbers_same_column)):
                    collected_results_by_line[key].extend(list(itertools.chain(*[is_aggregation((cell_index, aggregator), aggregatees, error_level)
                                                                                 for aggregatees in itertools.combinations(numbers_same_column, i + 1)])))
    else:
        raise RuntimeError('Illegal axis parameter for brute force approach.')


if __name__ == '__main__':
    luigi.run()
