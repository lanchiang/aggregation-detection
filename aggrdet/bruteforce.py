# Created by lan at 2021/3/2
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

from algorithm import NumberFormatNormalization


class Baseline(luigi.Task):
    dataset_path = luigi.Parameter()
    result_path = luigi.Parameter(default='./baseline/')
    error_level = luigi.FloatParameter(default=0)
    timeout = luigi.FloatParameter(default=300)
    verbose = luigi.BoolParameter(default=True, parsing=luigi.BoolParameter.EXPLICIT_PARSING)

    def output(self):
        return MockTarget('baseline')
        # return luigi.LocalTarget(os.path.join(self.result_path, 'baseline-results.jl'))

    def requires(self):
        return NumberFormatNormalization(self.dataset_path, self.result_path)

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
                file_dict['parameters']['timeout'] = self.timeout
                file_dict['parameters']['algorithm'] = self.__class__.__name__

                files_dict_map[(file_dict['file_name'], file_dict['table_id'])] = file_dict

        cpu_count = int(os.cpu_count() / 2)

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
                # print(file_name + '\t' + str(result['exec_time'][self.__class__.__name__]))

        with self.output().open('w') as file_writer:
            for file_output_dict in tqdm(files_dict_map.values(), desc='Serialize results'):
                file_writer.write(json.dumps(file_output_dict) + '\n')

    def detect_aggregations(self, file_json_dict):
        start_time = time.time()

        results_per_nf = {}
        for valid_nf, transformed_content in file_json_dict['valid_number_formats'].items():
            if valid_nf != file_json_dict['number_format']:
                continue
            numberized_indices = set()
            for index, value in np.ndenumerate(transformed_content):
                try:
                    Decimal(value)
                except decimal.InvalidOperation:
                    continue
                else:
                    numberized_indices.add(index)
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
                    results.extend(list(itertools.chain(*[self.is_aggregation((index, aggregator), aggregatees)
                                                          for aggregatees in itertools.combinations(numbers_same_row, i + 1)])))
                # check the same column
                column_index = index[1]
                numbers_same_column = [(elem, Decimal(transformed_content[elem[0]][elem[1]])) for elem in numberized_indices
                                       if elem[1] == column_index and elem != index]
                for i in range(1, len(numbers_same_column)):
                    results.extend(list(itertools.chain(*[self.is_aggregation((index, aggregator), aggee_comb)
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
        file_json_dict['detected_aggregations'] = detected_aggrs
        file_json_dict['exec_time'][self.__class__.__name__] = exec_time

        return file_json_dict

    def is_aggregation(self, aggregator, aggregatees):
        aggregation_results = []
        # sum?
        is_sum = False
        if aggregator[1] == 0.0:
            if abs(sum([e[1] for e in aggregatees]) - aggregator[1]) <= self.error_level:
                is_sum = True
        else:
            if abs((sum([e[1] for e in aggregatees]) - aggregator[1]) / aggregator[1]) <= self.error_level:
                is_sum = True
        if is_sum:
            aggregator_index = aggregator[0]
            aggregatee_indices = [e[0] for e in aggregatees]
            # aggregation_results.append((aggregator_index, aggregatee_indices, 'Sum'))
            aggregation_results.append({'aggregator_index': aggregator_index, 'aggregatee_indices': aggregatee_indices, 'operator': 'Sum'})

        return aggregation_results


if __name__ == '__main__':
    luigi.run()
