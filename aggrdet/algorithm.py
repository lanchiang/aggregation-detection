# Created by lan at 2021/2/2
import ast
import itertools
import json
import math
import os
import time
from concurrent.futures import TimeoutError
from copy import copy
from decimal import Decimal
from typing import List

import luigi
import numpy as np
from luigi.mock import MockTarget
from pebble import ProcessPool
from tqdm import tqdm

from approach.aggrdet._SumDetection import SumDetection
from bruteforce import delayed_bruteforce
from dataprep import NumberFormatNormalization
from elements import AggregationRelation, CellIndex, Direction, Cell
from helpers import is_empty_cell, hard_empty_cell_values
from tree import AggregationRelationForest


def remove_duplicates(collected_results_by_line):
    for tree, aggregations in collected_results_by_line.items():
        sorted_aggregations = []
        for aggregation in aggregations:
            sorted_ar = (aggregation[0], sorted(aggregation[1]), aggregation[2])
            sorted_aggregations.append(sorted_ar)
        deduplicated = []
        for aggregation in sorted_aggregations:
            if aggregation not in deduplicated:
                deduplicated.append(aggregation)
        collected_results_by_line[tree] = deduplicated


class Aggrdet(luigi.Task):
    dataset_path = luigi.Parameter()
    result_path = luigi.Parameter('./debug/')
    error_level = luigi.FloatParameter(default=0)
    error_strategy = luigi.Parameter(default='ratio')
    use_extend_strategy = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    use_delayed_bruteforce = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    timeout = luigi.FloatParameter(default=300)

    debug = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)

    def output(self):
        if self.debug:
            return luigi.LocalTarget(os.path.join(self.result_path, 'aggrdet.jl'))
        else:
            return MockTarget('aggrdet')

    def requires(self):
        return {'sum_detection': SumDetection(dataset_path=self.dataset_path, result_path=self.result_path, error_level=self.error_level,
                                              use_extend_strategy=self.use_extend_strategy,
                                              use_delayed_bruteforce=self.use_delayed_bruteforce, timeout=self.timeout, debug=self.debug)}

    def run(self):
        with self.input()['sum_detection'].open('r') as file_reader:
            sum_detection_results = [json.loads(line) for line in file_reader]

        result_dict = []
        for result in tqdm(sum_detection_results, desc='Select number format'):
            start_time = time.time()
            file_output_dict = copy(result)
            result_by_number_format = result['aggregation_detection_result']
            nf_cands = set(result_by_number_format.keys())
            nf_cands = sorted(list(nf_cands))
            if result['exec_time']['SumDetection'] < 0:
                pass
            if not bool(nf_cands):
                pass
            else:
                results = []
                for number_format in nf_cands:
                    row_wise_aggrs = result_by_number_format[number_format]
                    row_ar = set()
                    for r in row_wise_aggrs:
                        aggregator = ast.literal_eval(r[0])
                        aggregator = Cell(CellIndex(aggregator[0], aggregator[1]), None)
                        aggregatees = []
                        for e in r[1]:
                            aggregatees.append(ast.literal_eval(e))
                        aggregatees = [Cell(CellIndex(e[0], e[1]), None) for e in aggregatees]
                        aggregatees.sort()
                        row_ar.add(AggregationRelation(aggregator, tuple(aggregatees), None))
                    det_aggrs = row_ar
                    results.append((number_format, det_aggrs))
                results.sort(key=lambda x: len(x[1]), reverse=True)
                number_format = results[0][0]
                file_output_dict['detected_number_format'] = number_format
                det_aggrs = []
                for det_aggr in results[0][1]:
                    if isinstance(det_aggr, AggregationRelation):
                        aees = [[aee.cell_index.row_index, aee.cell_index.column_index] for aee in det_aggr.aggregatees]
                        aor = [det_aggr.aggregator.cell_index.row_index, det_aggr.aggregator.cell_index.column_index]
                        det_aggrs.append({'aggregator_index': aor, 'aggregatee_indices': aees})
                file_output_dict['detected_aggregations'] = det_aggrs
                file_output_dict.pop('aggregation_detection_result', None)
                try:
                    file_output_dict['number_formatted_values'] = file_output_dict['valid_number_formats'][number_format]
                except KeyError:
                    print()
                file_output_dict.pop('valid_number_formats', None)

            end_time = time.time()
            exec_time = end_time - start_time
            file_output_dict['exec_time'][self.__class__.__name__] = exec_time
            file_output_dict['parameters'] = {}
            file_output_dict['parameters']['error_level'] = self.error_level
            file_output_dict['parameters']['error_strategy'] = self.error_strategy
            file_output_dict['parameters']['use_extend_strategy'] = self.use_extend_strategy
            file_output_dict['parameters']['use_delayed_bruteforce_strategy'] = self.use_delayed_bruteforce
            file_output_dict['parameters']['timeout'] = self.timeout
            file_output_dict['parameters']['algorithm'] = self.__class__.__name__

            result_dict.append(file_output_dict)

        with self.output().open('w') as file_writer:
            for file_output_dict in result_dict:
                file_writer.write(json.dumps(file_output_dict) + '\n')
        pass


def eliminate_negative(table_value: np.ndarray):
    def numberize(value_str: str) -> float:
        try:
            number = float(value_str)
        except Exception:
            number = np.nan
        return number

    numberized_values = np.vectorize(numberize)(table_value).flatten()
    offset = np.min(numberized_values[~np.isnan(numberized_values)])

    if offset < 0:
        flattened_origin = table_value.flatten()
        non_negative_1d = np.where(np.isnan(numberized_values), flattened_origin, numberized_values - offset)
        non_negative_values = np.reshape(non_negative_1d, table_value.shape)
    else:
        offset = 0
        non_negative_values = np.copy(table_value)
    return non_negative_values, offset


def select_compatible_ar_cands(ar: AggregationRelation, ar_cands: List[AggregationRelation]):
    # conflict rule 1: bidirectional aggregation
    # survivor_cr1 = [ar_cand for ar_cand in ar_cands if not (ar_cand.aggregator == ar.aggregator and ar_cand.direction != ar.direction)]
    survivor_cr1 = []
    for ar_cand in ar_cands:
        same_aggregator = ar_cand.aggregator == ar.aggregator
        diff_direction = ar_cand.direction != ar.direction
        if not (same_aggregator and diff_direction):
            survivor_cr1.append(ar_cand)

    # conflict rule 2: complete inclusion
    survivor_cr2 = []
    for ar_cand in survivor_cr1:
        aggee_overlap = list(set(ar.aggregatees) & set(ar_cand.aggregatees))
        if ar.aggregator in ar_cand.aggregatees and aggee_overlap:
            continue
        if ar_cand.aggregator in ar.aggregatees and aggee_overlap:
            continue
        survivor_cr2.append(ar_cand)

    # conflict rule 3: partial aggregatees overlap
    survivor_cr3 = []
    for ar_cand in survivor_cr2:
        ar_aggee_set = set(ar.aggregatees)
        ar_cand_aggee_set = set(ar_cand.aggregatees)
        aggee_overlap = list(ar_aggee_set & ar_cand_aggee_set)
        if (len(ar_aggee_set) == len(aggee_overlap) and len(ar_cand_aggee_set) == len(aggee_overlap)) or len(aggee_overlap) == 0:
            survivor_cr3.append(ar_cand)

    return survivor_cr3


if __name__ == '__main__':
    luigi.run()
