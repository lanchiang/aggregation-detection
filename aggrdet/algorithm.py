# Created by lan at 2021/2/2
import ast
import json
import math
import os
import time
from copy import copy

import luigi
import numpy as np
from luigi.mock import MockTarget
from tqdm import tqdm

from approach.CollectiveAggregationDetectionTask import CollectiveAggregationDetectionTask
from approach.SupplementalAggregationDetectionTask import SupplementalAggregationDetectionTask
from approach.aggrdet.bruteforce import DelayedBruteforce
from approach.aggrdet.individual._AverageDetection import AverageDetection
from approach.aggrdet.individual._DivisionDetection import DivisionDetection
from approach.aggrdet.individual._RelativeChangeDetection import RelativeChangeDetection
from approach.aggrdet.supplemental._SupplementalSumDetection import SupplementalSumDetection
from elements import AggregationRelation, CellIndex, Cell
from helpers import AggregationOperator


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
    error_level_dict = luigi.DictParameter(default={'Sum': 0, 'Average': 0, 'Division': 0, 'RelativeChange': 0})
    target_aggregation_type = luigi.Parameter(default='All')
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
        return CollectiveAggregationDetectionTask(dataset_path=self.dataset_path, result_path=self.result_path,
                                                  error_level_dict=self.error_level_dict,
                                                  target_aggregation_type=self.target_aggregation_type,
                                                  use_extend_strategy=self.use_extend_strategy,
                                                  use_delayed_bruteforce=self.use_delayed_bruteforce, timeout=self.timeout, debug=self.debug)
        # return SupplementalAggregationDetectionTask(dataset_path=self.dataset_path, result_path=self.result_path,
        #                                             error_level_dict=self.error_level_dict,
        #                                             use_extend_strategy=self.use_extend_strategy,
        #                                             use_delayed_bruteforce=self.use_delayed_bruteforce, timeout=self.timeout, debug=self.debug)
        # if self.use_delayed_bruteforce:
        #     return DelayedBruteforce(dataset_path=self.dataset_path, result_path=self.result_path, error_level=self.error_level_dict,
        #                              target_aggregation_type=self.target_aggregation_type,
        #                              error_strategy=self.error_strategy, use_extend_strategy=self.use_extend_strategy, timeout=self.timeout, debug=self.debug)
        # else:
        #     sum_detector = {'sum_detector': SupplementalSumDetection(dataset_path=self.dataset_path, result_path=self.result_path,
        #                                                  error_level_dict=self.error_level_dict,
        #                                                  use_extend_strategy=self.use_extend_strategy,
        #                                                  use_delayed_bruteforce=self.use_delayed_bruteforce, timeout=self.timeout, debug=self.debug)}
        #     # Todo: replace with supplemental version
        #     average_detector = {'average_detector': AverageDetection(dataset_path=self.dataset_path, result_path=self.result_path,
        #                                                              error_level_dict=self.error_level_dict,
        #                                                              use_extend_strategy=self.use_extend_strategy,
        #                                                              use_delayed_bruteforce=self.use_delayed_bruteforce, timeout=self.timeout,
        #                                                              debug=self.debug)}
        #     division_detector = {
        #         'division_detector': DivisionDetection(dataset_path=self.dataset_path, result_path=self.result_path,
        #                                                error_level_dict=self.error_level_dict,
        #                                                use_extend_strategy=self.use_extend_strategy, use_delayed_bruteforce=self.use_delayed_bruteforce,
        #                                                timeout=self.timeout, debug=self.debug)}
        #     relative_change_detector = {
        #         'relative_change_detector': RelativeChangeDetection(dataset_path=self.dataset_path, result_path=self.result_path,
        #                                                             error_level_dict=self.error_level_dict,
        #                                                             use_extend_strategy=self.use_extend_strategy,
        #                                                             use_delayed_bruteforce=self.use_delayed_bruteforce,
        #                                                             timeout=self.timeout, debug=self.debug)}
        #
        #     all_detectors = {**sum_detector, **average_detector, **division_detector, **relative_change_detector}
        #
        #     required = {AggregationOperator.SUM.value: sum_detector,
        #                 AggregationOperator.AVERAGE.value: average_detector,
        #                 AggregationOperator.DIVISION.value: division_detector,
        #                 AggregationOperator.RELATIVE_CHANGE.value: relative_change_detector,
        #                 'All': all_detectors}.get(self.target_aggregation_type, None)
        #
        #     if required is None:
        #         raise RuntimeError('Given target aggregation type parameter is illegal.')
        #
        #     return required

    def run(self):
        # if self.use_delayed_bruteforce:
        #     with self.input().open('r') as file_reader:
        #         gathered_detection_results = [json.loads(line) for line in file_reader]
        # else:
        #     gathered_detection_results = {}
        #     for key, _ in self.input().items():
        #         with self.input()[key].open('r') as file_reader:
        #             file_dicts = [json.loads(line) for line in file_reader]
        #             for file_dict in file_dicts:
        #                 file_signature = (file_dict['file_name'], file_dict['table_id'])
        #                 if file_signature not in gathered_detection_results:
        #                     gathered_detection_results[file_signature] = file_dict
        #                 else:
        #                     gathered = gathered_detection_results[file_signature]['aggregation_detection_result']
        #                     for number_format in gathered.keys():
        #                         if number_format in gathered:
        #                             gathered[number_format].extend(file_dict['aggregation_detection_result'][number_format])
        #                         else:
        #                             gathered[number_format] = file_dict['aggregation_detection_result'][number_format]
        #
        #     gathered_detection_results = list(gathered_detection_results.values())
        # fuse_aggregation_results(gathered_detection_results)

        with self.input().open('r') as file_reader:
            gathered_detection_results = [json.loads(line) for line in file_reader]

        result_dict = []
        for result in tqdm(gathered_detection_results, desc='Select number format'):
            start_time = time.time()
            file_output_dict = copy(result)
            result_by_number_format = result['aggregation_detection_result']
            nf_cands = set(result_by_number_format.keys())
            nf_cands = sorted(list(nf_cands))
            if any([exec_time == math.nan for exec_time in result['exec_time'].values()]):
                # if result['exec_time']['SumDetection'] < 0:
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
                        row_ar.add(AggregationRelation(aggregator, tuple(aggregatees), r[2], None))
                    det_aggrs = row_ar
                    results.append((number_format, det_aggrs))
                results.sort(key=lambda x: len(x[1]), reverse=True)
                number_format = results[0][0]
                file_output_dict['detected_number_format'] = number_format
                det_aggrs = []
                for det_aggr in results[0][1]:
                    if isinstance(det_aggr, AggregationRelation):
                        aees = [(aee.cell_index.row_index, aee.cell_index.column_index) for aee in det_aggr.aggregatees]
                        aor = (det_aggr.aggregator.cell_index.row_index, det_aggr.aggregator.cell_index.column_index)
                        det_aggrs.append({'aggregator_index': aor, 'aggregatee_indices': aees, 'operator': det_aggr.operator})
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
            file_output_dict['parameters']['error_strategy'] = self.error_strategy
            file_output_dict['parameters']['algorithm'] = self.__class__.__name__

            result_dict.append(file_output_dict)

        with self.output().open('w') as file_writer:
            for file_output_dict in result_dict:
                file_writer.write(json.dumps(file_output_dict) + '\n')


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


if __name__ == '__main__':
    luigi.run()
