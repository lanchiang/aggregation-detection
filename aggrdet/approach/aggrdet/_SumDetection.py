# Created by lan at 2021/3/25
import decimal
import itertools
import json
import math
import os
import pprint
import time
from copy import copy
from decimal import Decimal

import luigi
import numpy as np
from luigi.mock import MockTarget
from pebble import ProcessPool
from tqdm import tqdm
from concurrent.futures import TimeoutError

from approach.aggrdet.detections import prune_conflict_ar_cands, remove_duplicates, remove_occasional
from approach.approach import AggregationDetection
from bruteforce import delayed_bruteforce
from elements import Cell, CellIndex, AggregationRelation, Direction
from helpers import is_empty_cell, hard_empty_cell_values
from tree import AggregationRelationForest


class SumDetection(AggregationDetection):

    def output(self):
        if self.debug:
            return luigi.LocalTarget(os.path.join(self.result_path, 'aggrdet-sum-detection.jl'))
        else:
            return MockTarget('aggregation-detection')

    def run(self):
        with self.input().open('r') as input_file:
            files_dict = [json.loads(line) for line in input_file]

            files_dict_map = {}
            for file_dict in files_dict:
                file_dict['detected_number_format'] = ''
                file_dict['detected_aggregations'] = []
                file_dict['aggregation_detection_result'] = {file_dict['number_format']: []}
                file_dict['exec_time'][self.__class__.__name__] = -1000
                file_dict['parameters'] = {}
                file_dict['parameters']['error_level'] = self.error_level
                file_dict['parameters']['use_extend_strategy'] = self.use_extend_strategy
                file_dict['parameters']['use_delayed_bruteforce_strategy'] = self.use_delayed_bruteforce
                file_dict['parameters']['timeout'] = self.timeout

                files_dict_map[(file_dict['file_name'], file_dict['table_id'])] = file_dict

            print('Detection sum aggregations.')

            with ProcessPool(max_workers=self.cpu_count, max_tasks=1) as pool:
                returned_result = pool.map(self.detect_aggregations, files_dict, timeout=self.timeout).result()

            while True:
                try:
                    result = next(returned_result)
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
                        merged_result['aggregation_detection_result'][nf].extend(column_wise_result['aggregation_detection_result'][nf])
                    merged_result['exec_time'][self.__class__.__name__] = \
                        merged_result['exec_time']['SumDetectionRowWise'] + merged_result['exec_time']['SumDetectionColumnWise']
                    files_dict_map[(file_name, sheet_name)] = merged_result

        with self.output().open('w') as file_writer:
            for file_output_dict in tqdm(files_dict_map.values(), desc='Serialize results'):
                file_writer.write(json.dumps(file_output_dict) + '\n')

    def detect_aggregations(self, file_dict):
        # if file_dict['file_name'] != 'fin_accounts.xls':
        #     raise ValueError
        row_wise_aggregations = self.detect_row_wise_aggregations(file_dict)
        column_wise_aggregations = self.detect_column_wise_aggregations(file_dict)
        return row_wise_aggregations, column_wise_aggregations

    def detect_proximity_aggregation_relations(self, forest: AggregationRelationForest, error_bound: float, error_strategy):
        roots = forest.get_roots()
        aggregation_candidates = []
        for index, root in enumerate(roots):
            try:
                aggregator_value = Decimal(root.value)
            except decimal.InvalidOperation:
                continue
            if aggregator_value.is_nan():
                continue
            if error_strategy == 'refrained' and aggregator_value == 0:
                continue

            # forward
            aggregatee_cells = []
            expected_sum = Decimal(0.0)
            is_equal = False
            current_lowest_error_level = math.inf
            for i in range(index + 1, len(roots) + 1):
                if i == len(roots):
                    if current_lowest_error_level <= error_bound:
                        aggregatee_cells.append(roots[i - 1])
                        is_equal = True
                    break

                # if this cell is empty, allows to continue
                # if is_empty_cell(roots[i].value):
                #     aggregatee_cells.append(roots[i])
                #     continue
                try:
                    aggregatee = Decimal(roots[i].value)
                except decimal.InvalidOperation:
                    if is_empty_cell(roots[i].value):
                        aggregatee_cells.append(roots[i])
                    continue
                    # if current_lowest_error_level <= error_bound:
                    #     aggregatee_cells.append(roots[i])
                    #     is_equal = True
                    # break
                else:
                    expected_sum += aggregatee if not aggregatee.is_nan() else Decimal(0.0)
                    aggregatee_cells.append(roots[i])
                    if error_strategy == 'ratio':
                        if aggregator_value == 0:
                            latest_error_level = abs(expected_sum - aggregator_value)
                        else:
                            latest_error_level = abs((expected_sum - aggregator_value) / aggregator_value)
                        if current_lowest_error_level < latest_error_level:
                            if current_lowest_error_level <= error_bound:
                                is_equal = True
                            break
                        else:
                            current_lowest_error_level = latest_error_level

                    elif error_strategy == 'value':
                        if abs(expected_sum - aggregator_value) <= error_bound:
                            is_equal = True
                            break
            if is_equal and len(aggregatee_cells) > 2:
                aggregatee_cells = aggregatee_cells[:len(aggregatee_cells) - 1]
                cursor = len(aggregatee_cells)
                for i in reversed(range(len(aggregatee_cells))):
                    if aggregatee_cells[i].value in hard_empty_cell_values:
                        cursor = i
                    else:
                        break
                aggregatee_cells = aggregatee_cells[:cursor]
                if len(aggregatee_cells) > 1:
                    if not (aggregator_value == 0 and all([aee_cell.value in hard_empty_cell_values or aee_cell.value == 0 for aee_cell in aggregatee_cells])):
                        ar = AggregationRelation(copy(root), tuple([copy(aee_cell) for aee_cell in aggregatee_cells]), Direction.FORWARD)
                        real_error_level = current_lowest_error_level
                        aggregation_candidates.append((ar, real_error_level))

            # backward
            aggregatee_cells = []
            expected_sum = Decimal(0.0)
            is_equal = False
            current_lowest_error_level = math.inf
            for i in range(index - 1, -2, -1):
                if i < 0:
                    if current_lowest_error_level <= error_bound:
                        aggregatee_cells.append(roots[0])
                        is_equal = True
                    break

                # if this cell is empty, allows to continue
                # if is_empty_cell(roots[i].value):
                #     aggregatee_cells.append(roots[i])
                #     continue
                try:
                    aggregatee = Decimal(roots[i].value)
                except decimal.InvalidOperation:
                    if is_empty_cell(roots[i].value):
                        aggregatee_cells.append(roots[i])
                    continue
                    # if current_lowest_error_level <= error_bound:
                    #     aggregatee_cells.append(roots[i])
                    #     is_equal = True
                    # break
                else:
                    expected_sum += aggregatee if not aggregatee.is_nan() else Decimal(0.0)
                    aggregatee_cells.append(roots[i])
                    if error_strategy == 'ratio':
                        if aggregator_value == 0:
                            latest_error_level = abs(expected_sum - aggregator_value)
                        else:
                            latest_error_level = abs((expected_sum - aggregator_value) / aggregator_value)
                        if current_lowest_error_level < latest_error_level:
                            if current_lowest_error_level <= error_bound:
                                is_equal = True
                            break
                        else:
                            current_lowest_error_level = latest_error_level
                    elif error_strategy == 'value':
                        if abs(expected_sum - aggregator_value) <= error_bound:
                            is_equal = True
                            break
            if is_equal and len(aggregatee_cells) > 2:
                aggregatee_cells = aggregatee_cells[:len(aggregatee_cells) - 1]
                cursor = len(aggregatee_cells)
                for i in reversed(range(len(aggregatee_cells))):
                    if aggregatee_cells[i].value in hard_empty_cell_values:
                        cursor = i
                    else:
                        break
                aggregatee_cells = aggregatee_cells[:cursor]
                if len(aggregatee_cells) > 1:
                    if not (aggregator_value == 0 and all([aee_cell.value in hard_empty_cell_values or aee_cell.value == 0 for aee_cell in aggregatee_cells])):
                        ar = AggregationRelation(copy(root), tuple([copy(aee_cell) for aee_cell in aggregatee_cells]), Direction.BACKWARD)
                        real_error_level = current_lowest_error_level
                        aggregation_candidates.append((ar, real_error_level))
        return aggregation_candidates

    # def detect_proximity_aggregation_relations(self, forest: AggregationRelationForest, error_bound: float, error_strategy):
    #     roots = forest.get_roots()
    #     aggregation_candidates = []
    #     for index, root in enumerate(roots):
    #         try:
    #             aggregator_value = Decimal(root.value)
    #         except decimal.InvalidOperation:
    #             continue
    #         if aggregator_value.is_nan():
    #             continue
    #         if error_strategy == 'refrained' and aggregator_value == 0:
    #             continue
    #
    #         # forward
    #         aggregatee_cells = []
    #         expected_sum = Decimal(0.0)
    #         is_equal = False
    #         current_lowest_error_level = math.inf
    #         for i in range(index + 1, len(roots)):
    #             # if i == len(roots):
    #             #     if current_lowest_error_level <= error_bound:
    #             #         aggregatee_cells.append(roots[i - 1])
    #             #         is_equal = True
    #             #     break
    #
    #             try:
    #                 aggregatee = Decimal(roots[i].value)
    #             except decimal.InvalidOperation:
    #                 if is_empty_cell(roots[i].value):
    #                     aggregatee_cells.append(roots[i])
    #                 continue
    #             else:
    #                 expected_sum += aggregatee
    #                 aggregatee_cells.append(roots[i])
    #                 if aggregator_value == 0:
    #                     latest_error_level = abs(expected_sum - aggregator_value)
    #                 else:
    #                     latest_error_level = abs((expected_sum - aggregator_value) / aggregator_value)
    #                 if current_lowest_error_level < latest_error_level:
    #                     if current_lowest_error_level <= error_bound:
    #                         aggregatee_cells = aggregatee_cells[:len(aggregatee_cells) - 1]
    #                         is_equal = True
    #                     break
    #                 else:
    #                     current_lowest_error_level = latest_error_level
    #             if i + 1 == len(roots):
    #                 if current_lowest_error_level <= error_bound:
    #                     is_equal = True
    #         if is_equal and len(aggregatee_cells) > 1:
    #             cursor = len(aggregatee_cells)
    #             for i in reversed(range(len(aggregatee_cells))):
    #                 if aggregatee_cells[i].value in hard_empty_cell_values:
    #                     cursor = i
    #                 else:
    #                     break
    #             aggregatee_cells = aggregatee_cells[:cursor]
    #             if len(aggregatee_cells) > 1:
    #                 if not (aggregator_value == 0 and all([aee_cell.value in hard_empty_cell_values or aee_cell.value == 0 for aee_cell in aggregatee_cells])):
    #                     ar = AggregationRelation(copy(root), tuple([copy(aee_cell) for aee_cell in aggregatee_cells]), Direction.FORWARD)
    #                     real_error_level = current_lowest_error_level
    #                     aggregation_candidates.append((ar, real_error_level))
    #
    #         # backward
    #         aggregatee_cells = []
    #         expected_sum = Decimal(0.0)
    #         is_equal = False
    #         current_lowest_error_level = math.inf
    #         for i in reversed(range(index)):
    #         # for i in range(index - 1, -2, -1):
    #         #     if i < 0:
    #                 # if current_lowest_error_level <= error_bound:
    #                 #     aggregatee_cells.append(roots[0])
    #                 #     is_equal = True
    #                 # break
    #
    #             try:
    #                 aggregatee = Decimal(roots[i].value)
    #             except decimal.InvalidOperation:
    #                 if is_empty_cell(roots[i].value):
    #                     aggregatee_cells.append(roots[i])
    #                 continue
    #             else:
    #                 expected_sum += aggregatee
    #                 aggregatee_cells.append(roots[i])
    #                 if aggregator_value == 0:
    #                     latest_error_level = abs(expected_sum - aggregator_value)
    #                 else:
    #                     latest_error_level = abs((expected_sum - aggregator_value) / aggregator_value)
    #                 if current_lowest_error_level < latest_error_level:
    #                     if current_lowest_error_level <= error_bound:
    #                         aggregatee_cells = aggregatee_cells[:len(aggregatee_cells) - 1]
    #                         is_equal = True
    #                     break
    #                 else:
    #                     current_lowest_error_level = latest_error_level
    #             if i == 0:
    #                 if current_lowest_error_level <= error_bound:
    #                     is_equal = True
    #         if is_equal and len(aggregatee_cells) > 1:
    #             cursor = len(aggregatee_cells)
    #             for i in reversed(range(len(aggregatee_cells))):
    #                 if aggregatee_cells[i].value in hard_empty_cell_values:
    #                     cursor = i
    #                 else:
    #                     break
    #             aggregatee_cells = aggregatee_cells[:cursor]
    #             if len(aggregatee_cells) > 1:
    #                 if not (aggregator_value == 0 and all([aee_cell.value in hard_empty_cell_values or aee_cell.value == 0 for aee_cell in aggregatee_cells])):
    #                     ar = AggregationRelation(copy(root), tuple([copy(aee_cell) for aee_cell in aggregatee_cells]), Direction.BACKWARD)
    #                     real_error_level = current_lowest_error_level
    #                     aggregation_candidates.append((ar, real_error_level))
    #     return aggregation_candidates

    def detect_row_wise_aggregations(self, file_dict):
        start_time = time.time()
        _file_dict = copy(file_dict)
        _file_dict['aggregation_detection_result'] = {}
        for number_format in _file_dict['valid_number_formats']:

            # Todo: just for fair timeout comparison
            if number_format != _file_dict['number_format']:
                continue

            table_value = np.array(_file_dict['valid_number_formats'][number_format])

            file_cells = np.full_like(table_value, fill_value=table_value, dtype=object)
            for index, value in np.ndenumerate(table_value):
                file_cells[index] = Cell(CellIndex(index[0], index[1]), value)

            forests_by_rows = [AggregationRelationForest(row_cells) for row_cells in file_cells]
            forest_by_row_index = {}
            for index, forest in enumerate(forests_by_rows):
                forest_by_row_index[index] = forest
            collected_results_by_row = {}
            while True:
                ar_cands_by_row = [(self.detect_proximity_aggregation_relations(forest, self.error_level, 'ratio'), forest) for forest in
                                   forests_by_rows]
                # pprint.pprint(ar_cands_by_row)
                # print()
                # get all non empty ar_cands
                ar_cands_by_row = list(filter(lambda x: bool(x[0]), ar_cands_by_row))
                if not ar_cands_by_row:
                    break

                forest_indexed_by_ar_cand = {}
                for ar_cands, forest in ar_cands_by_row:
                    for ar_cand in ar_cands:
                        forest_indexed_by_ar_cand[ar_cand[0]] = forest

                ar_cands_by_row, forests_by_rows = list(zip(*ar_cands_by_row))

                ar_cands_by_column_index = prune_conflict_ar_cands(ar_cands_by_row, axis=0)

                if not bool(ar_cands_by_column_index):
                    break

                for _, ar_cands in ar_cands_by_column_index.items():
                    for i in range(len(ar_cands)):
                        ar_cands[i] = (ar_cands[i], forest_indexed_by_ar_cand[ar_cands[i]])

                extended_ar_cands_w_forest = []
                for ar_column_indices, ar_cands_w_forest in ar_cands_by_column_index.items():
                    [forest.consume_relation(ar_cand) for ar_cand, forest in ar_cands_w_forest]
                    confirmed_ars_row_index = [ar_cand.aggregator.cell_index.row_index for ar_cand, _ in ar_cands_w_forest]
                    if self.use_extend_strategy:
                        num_rows = file_cells.shape[0]
                        index_aggregator = ar_column_indices[0]
                        for i in range(num_rows):
                            # create an extended aggregation and make it consumed by the forest for this row
                            extended_ar_cand_aggor = Cell(CellIndex(i, index_aggregator), table_value[i, index_aggregator])
                            extended_ar_cand_aggees = tuple([Cell(CellIndex(i, j), table_value[i, j]) for j in ar_column_indices[1]])
                            extended_ar_cand_direction = ar_cands_w_forest[0][0].direction
                            extended_ar_cand = AggregationRelation(extended_ar_cand_aggor, extended_ar_cand_aggees, extended_ar_cand_direction)
                            extended_ar_cands_w_forest.append((extended_ar_cand, forest_by_row_index[i]))

                            if i in confirmed_ars_row_index or i not in forest_by_row_index:
                                continue
                            try:
                                float(table_value[i, index_aggregator])
                            except Exception:
                                continue
                            else:
                                forest_by_row_index[i].consume_relation(extended_ar_cand)

                for _, ar_cands_w_forest in ar_cands_by_column_index.items():
                    [forest.remove_consumed_aggregator(ar_cand) for ar_cand, forest in ar_cands_w_forest]
                if self.use_extend_strategy:
                    [forest.remove_consumed_aggregator(ar_cand) for ar_cand, forest in extended_ar_cands_w_forest]

            for _, forest in forest_by_row_index.items():
                results_dict = forest.results_to_str('Sum')
                collected_results_by_row[forest] = results_dict

            if self.use_delayed_bruteforce:
                delayed_bruteforce(collected_results_by_row, table_value, self.error_level, axis=0)
                remove_duplicates(collected_results_by_row)
                collected_results = remove_occasional(collected_results_by_row, axis=0)
                # collected_results = list(itertools.chain(*[results_dict for _, results_dict in collected_results_by_row.items()]))
            else:
                collected_results = list(itertools.chain(*[results_dict for _, results_dict in collected_results_by_row.items()]))

            # pprint.pprint(collected_results)
            # print()

            _file_dict['aggregation_detection_result'][number_format] = collected_results
        end_time = time.time()
        exec_time = end_time - start_time
        _file_dict['exec_time']['SumDetectionRowWise'] = exec_time
        return _file_dict

    def detect_column_wise_aggregations(self, file_dict):
        start_time = time.time()
        _file_dict = copy(file_dict)
        _file_dict['aggregation_detection_result'] = {}
        for number_format in _file_dict['valid_number_formats']:
            # Todo: just for fair timeout comparison
            if number_format != _file_dict['number_format']:
                continue

            # table_value = np.array(_file_dict['table_array'])
            table_value = np.array(_file_dict['valid_number_formats'][number_format])

            # agg_cands = pool.starmap(self.process_row, zip(non_negative_values, range(len(table_value))))
            file_cells = np.full_like(table_value, fill_value=table_value, dtype=object)
            for index, value in np.ndenumerate(table_value):
                file_cells[index] = Cell(CellIndex(index[0], index[1]), value)

            forests_by_columns = [AggregationRelationForest(file_cells[:, i]) for i in range(file_cells.shape[1])]
            forest_by_column_index = {}
            for index, forest in enumerate(forests_by_columns):
                forest_by_column_index[index] = forest
            collected_results_by_column = {}
            while True:
                ar_cands_by_column = [(self.detect_proximity_aggregation_relations(forest, self.error_level, 'ratio'), forest) for forest in
                                      forests_by_columns]
                # get all non empty ar_cands
                ar_cands_by_column = list(filter(lambda x: bool(x[0]), ar_cands_by_column))
                if not ar_cands_by_column:
                    break

                forest_indexed_by_ar_cand = {}
                for ar_cands, forest in ar_cands_by_column:
                    for ar_cand in ar_cands:
                        forest_indexed_by_ar_cand[ar_cand[0]] = forest

                ar_cands_by_column, forests_by_columns = list(zip(*ar_cands_by_column))

                ar_cands_by_row_index = prune_conflict_ar_cands(ar_cands_by_column, axis=1)

                # prune the candidates that do not appear in X% of all rows. X equals to satisfied_vote_ratio
                # ar_cands_by_column = prune_occasional_ar_cands(ar_cands_by_column, self.satisfied_vote_ratio, axis=1)

                if not bool(ar_cands_by_row_index):
                    break

                for _, ar_cands in ar_cands_by_row_index.items():
                    for i in range(len(ar_cands)):
                        ar_cands[i] = (ar_cands[i], forest_indexed_by_ar_cand[ar_cands[i]])

                extended_ar_cands_w_forest = []
                for ar_row_indices, ar_cands_w_forest in ar_cands_by_row_index.items():
                    [forest.consume_relation(ar_cand) for ar_cand, forest in ar_cands_w_forest]
                    confirmed_ars_column_index = [ar_cand.aggregator.cell_index.column_index for ar_cand, _ in ar_cands_w_forest]
                    if self.use_extend_strategy:
                        num_columns = file_cells.shape[1]
                        index_aggregator = ar_row_indices[0]
                        for i in range(num_columns):
                            # create an extended aggregation and make it consumed by the forest for this column
                            extended_ar_cand_aggor = Cell(CellIndex(index_aggregator, i), table_value[index_aggregator, i])
                            extended_ar_cand_aggees = tuple([Cell(CellIndex(j, i), table_value[j, i]) for j in ar_row_indices[1]])
                            extended_ar_cand_direction = ar_cands_w_forest[0][0].direction
                            extended_ar_cand = AggregationRelation(extended_ar_cand_aggor, extended_ar_cand_aggees, extended_ar_cand_direction)
                            extended_ar_cands_w_forest.append((extended_ar_cand, forest_by_column_index[i]))

                            if i in confirmed_ars_column_index or i not in forest_by_column_index:
                                continue
                            try:
                                float(table_value[index_aggregator, i])
                            except Exception:
                                continue
                            else:
                                # create an extended aggregation and make it consumed by the forest for this column
                                forest_by_column_index[i].consume_relation(extended_ar_cand)

                # for row_index, ar_cands_w_forest in ar_cands_by_row_index.items():
                #     [forest.consume_relation(ar_cand) for ar_cand, forest in ar_cands_w_forest]
                for _, ar_cands_w_forest in ar_cands_by_row_index.items():
                    [forest.remove_consumed_aggregator(ar_cand) for ar_cand, forest in ar_cands_w_forest]
                if self.use_extend_strategy:
                    [forest.remove_consumed_aggregator(ar_cand) for ar_cand, forest in extended_ar_cands_w_forest]

            for _, forest in forest_by_column_index.items():
                results_dict = forest.results_to_str('Sum')
                collected_results_by_column[forest] = results_dict

            if self.use_delayed_bruteforce:
                delayed_bruteforce(collected_results_by_column, table_value, self.error_level, axis=1)
                remove_duplicates(collected_results_by_column)
                collected_results = remove_occasional(collected_results_by_column, axis=1)
                # collected_results = list(itertools.chain(*[results_dict for _, results_dict in collected_results_by_column.items()]))
            else:
                collected_results = list(itertools.chain(*[results_dict for _, results_dict in collected_results_by_column.items()]))

            # collected_results = list(itertools.chain(*[results_dict for _, results_dict in collected_results_by_column.items()]))
            _file_dict['aggregation_detection_result'][number_format] = collected_results
        end_time = time.time()
        exec_time = end_time - start_time
        _file_dict['exec_time']['SumDetectionColumnWise'] = exec_time
        return _file_dict