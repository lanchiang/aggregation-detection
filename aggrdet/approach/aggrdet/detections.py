# Created by lan at 2021/3/12
import ast
import itertools
import json
import math
import os
import time
from copy import copy
from decimal import Decimal, InvalidOperation

import luigi
import numpy as np
from luigi.mock import MockTarget
from pebble import ProcessPool
from tqdm import tqdm
from concurrent.futures import TimeoutError

from approach.approach import AggregationDetection
from bruteforce import delayed_bruteforce
from elements import Cell, CellIndex, AggregationRelation, Direction
from helpers import is_empty_cell, hard_empty_cell_values, AggregationOperator
from number import str2decimal
from tree import AggregationRelationForest


def prune_conflict_ar_cands(ar_cands_by_line, axis=0):
    satisfied_cands_index = {}
    for ar_cands in ar_cands_by_line:
        for ar_cand, error_level in ar_cands:
            aggregator = ar_cand.aggregator
            aggregatees = ar_cand.aggregatees
            if axis == 0:
                ar_tuple = (aggregator.cell_index.column_index, tuple([aggregatee.cell_index.column_index for aggregatee in aggregatees]))
            else:
                ar_tuple = (aggregator.cell_index.row_index, tuple([aggregatee.cell_index.row_index for aggregatee in aggregatees]))
            if ar_tuple not in satisfied_cands_index:
                satisfied_cands_index[ar_tuple] = []
            satisfied_cands_index[ar_tuple].append((ar_cand, error_level))
    satisfied_cands_index = {k: v for k, v in
                             sorted(satisfied_cands_index.items(), key=lambda item: (len(item[1]), - sum([el for _, el in item[1]]) / len(item[1])),
                                    reverse=True)}
    satisfied_cands_index = {k: [e[0] for e in v] for k, v in satisfied_cands_index.items()}

    non_conflict_ar_cands_index = copy(satisfied_cands_index)
    for ar_index in satisfied_cands_index:
        if ar_index not in non_conflict_ar_cands_index:
            continue
        non_conflict_ar_cands_index[ar_index] = satisfied_cands_index[ar_index]
        non_conflicts = filter_conflict_ar_cands(ar_index, non_conflict_ar_cands_index.keys())
        non_conflict_ar_cands_index = {}
        for non_conflict_indx in non_conflicts:
            non_conflict_ar_cands_index[non_conflict_indx] = satisfied_cands_index[non_conflict_indx]
    return non_conflict_ar_cands_index


def filter_conflict_ar_cands(ar_index, list_ar_cands_index):
    # conflict rule 1: bidirectional aggregation
    survivor_cr1 = []
    for ar_cand_index in list_ar_cands_index:
        if ar_index[0] == ar_cand_index[0]:
            if (ar_cand_index[0] - ar_cand_index[1][0]) * (ar_index[0] - ar_index[1][0]) > 0:
                survivor_cr1.append(ar_cand_index)
        else:
            survivor_cr1.append(ar_cand_index)

    # conflict rule 2: complete inclusion
    survivor_cr2 = []
    for ar_cand_index in survivor_cr1:
        aggee_overlap = list(set(ar_index[1]) & set(ar_cand_index[1]))
        if ar_index[0] in ar_cand_index[1] and bool(aggee_overlap):
            continue
        if ar_cand_index[0] in ar_index[1] and bool(aggee_overlap):
            continue
        survivor_cr2.append(ar_cand_index)

    # conflict rule 3: partial aggregatees overlap
    survivor_cr3 = []
    for ar_cand_index in survivor_cr2:
        ar_aggee_set = set(ar_index[1])
        ar_cand_aggee_set = set(ar_cand_index[1])
        aggee_overlap = list(ar_aggee_set & ar_cand_aggee_set)
        if (len(ar_aggee_set) == len(aggee_overlap) and len(ar_cand_aggee_set) == len(aggee_overlap)) or len(aggee_overlap) == 0:
            survivor_cr3.append(ar_cand_index)
    return survivor_cr3


def detect_proximity_aggregation_relations(forest: AggregationRelationForest, error_bound: float, error_strategy) -> list:
    """
    find all proximity aggregation candidates for the given row
    :param forest:
    :return:
    """
    roots = forest.get_roots()
    aggregation_candidates = []
    for index, root in enumerate(roots):
        try:
            aggregator_value = Decimal(root.value)
        except Exception:
            continue
        if aggregator_value.is_nan():
            continue
        if error_strategy == 'refrained' and aggregator_value == 0:
            continue

        # forward
        aggregatee_cells = []
        sum = Decimal(0.0)
        is_equal = False
        current_lowest_error_level = math.inf
        for i in range(index + 1, len(roots) + 1):
            if i == len(roots):
                if current_lowest_error_level <= error_bound:
                    aggregatee_cells.append(roots[i - 1])
                    is_equal = True
                break

            # if this cell is empty, allows to continue
            if is_empty_cell(roots[i].value):
                # Todo: if this cell is added to the aggregatee set,
                #  later it should be concerned when comparing to the groundtruth: either having it or not should be tried matching
                aggregatee_cells.append(roots[i])
                continue
            try:
                aggregatee = Decimal(roots[i].value)
            except Exception:
                if current_lowest_error_level <= error_bound:
                    aggregatee_cells.append(roots[i])
                    is_equal = True
                break
            else:
                sum += aggregatee
                aggregatee_cells.append(roots[i])
                if error_strategy == 'ratio':
                    if aggregator_value == 0:
                        latest_error_level = abs(sum - aggregator_value)
                        # if sum == aggregator_value:
                        #     is_equal = True
                        #     break
                    else:
                        latest_error_level = abs((sum - aggregator_value) / aggregator_value)
                        # if abs((sum - aggregator_value) / aggregator_value) <= error_bound:
                        #     is_equal = True
                        #     break
                    if current_lowest_error_level < latest_error_level:
                        if current_lowest_error_level <= error_bound:
                            is_equal = True
                        break
                    else:
                        current_lowest_error_level = latest_error_level

                elif error_strategy == 'value':
                    if abs(sum - aggregator_value) <= error_bound:
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
        sum = Decimal(0.0)
        is_equal = False
        current_lowest_error_level = math.inf
        for i in range(index - 1, -2, -1):
            if i < 0:
                if current_lowest_error_level <= error_bound:
                    aggregatee_cells.append(roots[0])
                    is_equal = True
                break

            # if this cell is empty, allows to continue
            if is_empty_cell(roots[i].value):
                aggregatee_cells.append(roots[i])
                continue
            try:
                aggregatee = Decimal(roots[i].value)
            except Exception:
                if current_lowest_error_level <= error_bound:
                    aggregatee_cells.append(roots[i])
                    is_equal = True
                break
            else:
                sum += aggregatee
                aggregatee_cells.append(roots[i])
                if error_strategy == 'ratio':
                    if aggregator_value == 0:
                        latest_error_level = abs(sum - aggregator_value)
                        # if sum == aggregator_value:
                        #     is_equal = True
                        #     break
                    else:
                        latest_error_level = abs((sum - aggregator_value) / aggregator_value)
                        # if abs((sum - aggregator_value) / aggregator_value) <= error_bound:
                        #     is_equal = True
                        #     break
                    if current_lowest_error_level < latest_error_level:
                        if current_lowest_error_level <= error_bound:
                            is_equal = True
                        break
                    else:
                        current_lowest_error_level = latest_error_level
                elif error_strategy == 'value':
                    if abs(sum - aggregator_value) <= error_bound:
                        is_equal = True
                        break
        if is_equal and len(aggregatee_cells) > 2:
            aggregatee_cells = aggregatee_cells[:len(aggregatee_cells) - 1]
            cursor = -1
            for i in range(len(aggregatee_cells)):
                if aggregatee_cells[i].value in hard_empty_cell_values:
                    cursor = i
                else:
                    break
            aggregatee_cells = aggregatee_cells[cursor + 1:]
            if len(aggregatee_cells) > 1:
                if not (aggregator_value == 0 and all([aee_cell.value in hard_empty_cell_values or aee_cell.value == 0 for aee_cell in aggregatee_cells])):
                    ar = AggregationRelation(copy(root), tuple([copy(aee_cell) for aee_cell in aggregatee_cells]), Direction.BACKWARD)
                    real_error_level = current_lowest_error_level
                    aggregation_candidates.append((ar, real_error_level))
    return aggregation_candidates


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


def remove_occasional(collected_results_by_line, axis):
    """
    Remove the detected aggregations that appear occasionally. Take row-wise aggregation detection as example, a detected aggregation is occasional,
    if none of the other candidates with the same column combinations are also aggregations.

    :param collected_results_by_line: results of delayed brute-force aggregation.
    :param axis: row-wise check if 0, column-wise otherwise.
    :return: occasional-case-free results
    """
    if axis == 0:
        # results are row wise
        results_by_column_combinations = {}
        for _, aggregations in collected_results_by_line.items():
            for aggregation in aggregations:
                if aggregation[2] == AggregationOperator.SUM.value:
                    aggregator_partial_index = ast.literal_eval(aggregation[0])[1]
                    aggregatee_partial_indices = tuple(sorted([ast.literal_eval(aggregatee)[1] for aggregatee in aggregation[1]]))
                    combination_partial_signature = (aggregator_partial_index, aggregatee_partial_indices)
                    # print(combination_partial_signature)
                    if combination_partial_signature not in results_by_column_combinations:
                        results_by_column_combinations[combination_partial_signature] = []
                    results_by_column_combinations[combination_partial_signature].append(aggregation)
        # print(results_by_column_combinations)
        # print(len(list(itertools.chain(*[aggregations for _, aggregations in results_by_column_combinations.items()]))))
        non_occasional_aggregations = [aggregations for _, aggregations in results_by_column_combinations.items() if len(aggregations) > 1]
        # print(len(non_occasional_aggregations))
        pass
    else:
        # results are column wise
        results_by_row_combinations = {}
        for _, aggregations in collected_results_by_line.items():
            for aggregation in aggregations:
                if aggregation[2] == AggregationOperator.SUM.value:
                    aggregator_partial_index = ast.literal_eval(aggregation[0])[0]
                    aggregatee_partial_indices = tuple(sorted([ast.literal_eval(aggregatee)[0] for aggregatee in aggregation[1]]))
                    combination_partial_signature = (aggregator_partial_index, aggregatee_partial_indices)
                    if combination_partial_signature not in results_by_row_combinations:
                        results_by_row_combinations[combination_partial_signature] = []
                    results_by_row_combinations[combination_partial_signature].append(aggregation)
        non_occasional_aggregations = [aggregations for _, aggregations in results_by_row_combinations.items() if len(aggregations) > 1]

    non_occasional_aggregations = list(itertools.chain(*non_occasional_aggregations))
    return non_occasional_aggregations


def rank_numeric_cells(file_values, axis=0):
    """
    Rank the numeric cells of each row or column based on their values, ascending.
    This function returns an array with the same size as the given file value array. Numeric values in the original array are replaced by its ranking along the
    required axis, while non-numeric values are replaced with None.

    :param file_values: the given file values
    :param axis: rank along rows if 0, columns otherwise
    :return: an array that stores ranking of numeric values.
    """
    ranking_array = np.full_like(file_values, None)
    if axis == 0:
        # rank alongside rows
        # for row_index, row_values in enumerate(file_values):
        #     numeric_values = [((row_index, column_index), cell_value) for column_index, cell_value in enumerate(row_values) if str2decimal(cell_value, None) is not None]
        #     sorted_numeric_values = sorted(numeric_values, key=lambda x: Decimal(x[1]))
        #     for index, numeric_value in enumerate(sorted_numeric_values):
        #         ranking_array[numeric_value[0]] = index
        pass
    else:
        # rank alongside columns
        pass

    return ranking_array


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

            cpu_count = int(os.cpu_count() * 0.75)

            with ProcessPool(max_workers=cpu_count, max_tasks=1) as pool:
                returned_result = pool.map(self.detect_aggregations, files_dict, timeout=self.timeout).result()

            while True:
                try:
                    result = next(returned_result)
                except StopIteration:
                    break
                except TimeoutError:
                    pass
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
        # if file_dict['file_name'] != '00sumdat.xls':
        #     return None, None
        row_wise_aggregations = self.detect_row_wise_aggregations(file_dict)
        column_wise_aggregations = self.detect_column_wise_aggregations(file_dict)
        return row_wise_aggregations, column_wise_aggregations

    def detect_row_wise_aggregations(self, file_dict):
        start_time = time.time()
        _file_dict = copy(file_dict)
        _file_dict['aggregation_detection_result'] = {}
        for number_format in _file_dict['valid_number_formats']:

            # Todo: just for fair timeout comparison
            if number_format != _file_dict['number_format']:
                continue

            table_value = np.array(_file_dict['valid_number_formats'][number_format])

            ranking_file_array = rank_numeric_cells(table_value, axis=0)

            file_cells = np.full_like(table_value, fill_value=table_value, dtype=object)
            for index, value in np.ndenumerate(table_value):
                file_cells[index] = Cell(CellIndex(index[0], index[1]), value)

            forests_by_rows = [AggregationRelationForest(row_cells) for row_cells in file_cells]
            forest_by_row_index = {}
            for index, forest in enumerate(forests_by_rows):
                forest_by_row_index[index] = forest
            collected_results_by_row = {}
            while True:
                ar_cands_by_row = [(detect_proximity_aggregation_relations(forest, self.error_level, 'ratio'), forest) for forest in
                                   forests_by_rows]
                # get all non empty ar_cands
                ar_cands_by_row = list(filter(lambda x: bool(x[0]), ar_cands_by_row))
                if not ar_cands_by_row:
                    break

                forest_indexed_by_ar_cand = {}
                for ar_cands, forest in ar_cands_by_row:
                    for ar_cand in ar_cands:
                        forest_indexed_by_ar_cand[ar_cand[0]] = forest

                ar_cands_by_row, forests_by_rows = list(zip(*ar_cands_by_row))
                # [print(elem) for elem in ar_cands_by_row]

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
                delayed_bruteforce(collected_results_by_row, table_value, ranking_file_array, self.error_level, axis=0)
                remove_duplicates(collected_results_by_row)
                collected_results = remove_occasional(collected_results_by_row, axis=0)
                # collected_results = list(itertools.chain(*[results_dict for _, results_dict in collected_results_by_row.items()]))
            else:
                collected_results = list(itertools.chain(*[results_dict for _, results_dict in collected_results_by_row.items()]))

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

            ranking_file_array = rank_numeric_cells(table_value, axis=1)

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
                ar_cands_by_column = [(detect_proximity_aggregation_relations(forest, self.error_level, 'ratio'), forest) for forest in
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
                delayed_bruteforce(collected_results_by_column, table_value, ranking_file_array, self.error_level, axis=1)
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
