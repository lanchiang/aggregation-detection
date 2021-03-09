# Created by lan at 2021/2/2
import ast
import gzip
import itertools
import json
import math
import os
import time
from collections import OrderedDict
from copy import deepcopy, copy
from decimal import Decimal
from multiprocessing import Pool, cpu_count
from multiprocessing import current_process
from typing import List

import luigi
import numpy as np
from luigi.mock import MockTarget
from tqdm import tqdm

from data import detect_number_format, transform_number_format
from dataprep import DataPreparation
from elements import AggregationRelation, CellIndex, Direction, Cell
from helpers import is_empty_cell, hard_empty_cell_values
from hierarchy import HierarchyForest
from tree import AggregationRelationForest


def detect_error_bound(file_value: np.ndarray) -> float:
    numbers = []
    for index, value in np.ndenumerate(file_value):
        try:
            # Todo: Use Decimal instead
            number = float(value)
        except Exception:
            pass
        else:
            numbers.append(abs(number))
    min_number = min(numbers)
    # min_number = min_number - int(min_number)
    if min_number < 1:
        error_bound = 0.01
    elif min_number < 10:
        error_bound = 0.1
    else:
        error_bound = 1
    return error_bound


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


def prune_occasional_ar_cands(ar_cands_by_row, satisfied_ratio, axis=0):
    """
    Create an inverted indexing for the ar_cands_by_row variable. Key is <Aggregator, Aggregations> and value is the row indices
    :param ar_cands_by_row:
    :param satisfied_ratio:
    :param axis specifies either row-wise or column-wise should the candidates be checked. 0 means row-wise
    :return:
    """

    # this set stores <Aggregator, Aggregatees> that have been confirmed satisfied. Satisfied cands must appear in more than half of the rows.
    satisfied_cands = {}
    for ar_cands in ar_cands_by_row:
        for ar_cand in ar_cands:
            aggregator = ar_cand.aggregator
            aggregatees = ar_cand.aggregatees
            if axis == 0:
                ar_tuple = (aggregator.cell_index.column_index, tuple([aggregatee.cell_index.column_index for aggregatee in aggregatees]))
            else:
                ar_tuple = (aggregator.cell_index.row_index, tuple([aggregatee.cell_index.row_index for aggregatee in aggregatees]))
            if ar_tuple not in satisfied_cands:
                satisfied_cands[ar_tuple] = []
            satisfied_cands[ar_tuple].append(ar_cand)

    pruned_ar_cands_by_rows = []
    for ar_cands in ar_cands_by_row:
        pruned_ar_cands = []
        for ar_cand in ar_cands:
            aggregator = ar_cand.aggregator
            aggregatees = ar_cand.aggregatees
            if axis == 0:
                ar_tuple = (aggregator.cell_index.column_index, tuple([aggregatee.cell_index.column_index for aggregatee in aggregatees]))
            else:
                ar_tuple = (aggregator.cell_index.row_index, tuple([aggregatee.cell_index.row_index for aggregatee in aggregatees]))
            if len(satisfied_cands[ar_tuple]) / len(ar_cands_by_row) >= satisfied_ratio:
                # if len(satisfied_cands[ar_tuple]) / size >= satisfied_ratio:
                pruned_ar_cands.append(ar_cand)
        pruned_ar_cands_by_rows.append(pruned_ar_cands)
    return pruned_ar_cands_by_rows


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
        if ar_index[0] in ar_cand_index[1] and aggee_overlap:
            continue
        if ar_cand_index[0] in ar_index[1] and aggee_overlap:
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
                ar = AggregationRelation(copy(root), tuple([copy(aee_cell) for aee_cell in aggregatee_cells]), Direction.BACKWARD)
                real_error_level = current_lowest_error_level
                aggregation_candidates.append((ar, real_error_level))
    return aggregation_candidates


class NumberFormatNormalization(luigi.Task):
    """
    This task runs the number format normalization task on the initial input file, and produces a set of valid number formats on each file.
    """

    dataset_path = luigi.Parameter()
    result_path = luigi.Parameter()
    sample_ratio = luigi.FloatParameter(default=0.1)

    def output(self):
        return MockTarget(fn='number-format-normalization')
        # return luigi.LocalTarget(os.path.join(self.result_path, 'file-number-format.jl'))

    def requires(self):
        return DataPreparation(self.dataset_path, self.result_path)

    def run(self):
        with self.input().open('r') as input_file:
            dump_json_string = []
            files_dict = [json.loads(line) for line in input_file]
            # for line in input_file:
            for file_json_dict in tqdm(files_dict, desc='Number format selection.'):
                start_time = time.time()
                # file_json_dict = json.loads(line)
                file_value_array = np.array(file_json_dict['table_array'])
                # if file_json_dict['file_name'] != 'C10015':
                #     continue
                number_format = detect_number_format(file_value_array)
                # print(number_format)
                tnff = {}
                for nf in number_format:
                    tnff[nf] = transform_number_format(file_json_dict['table_array'], nf)
                file_json_dict['valid_number_formats'] = tnff

                end_time = time.time()
                exec_time = end_time - start_time
                file_json_dict['exec_time'][self.__class__.__name__] = exec_time

                dump_json_string.append(json.dumps(file_json_dict))

        with self.output().open('w') as file_writer:
            for djs in dump_json_string:
                file_writer.write(djs + '\n')


class SumDetectionRowWise(luigi.Task):
    dataset_path = luigi.Parameter()
    result_path = luigi.Parameter()
    error_bound = luigi.FloatParameter(default=1)
    satisfied_vote_ratio = luigi.FloatParameter(default=0.5)
    error_strategy = luigi.Parameter(default='ratio')
    use_extend_strategy = luigi.BoolParameter(default=False)

    def output(self):
        return MockTarget('row-wise-detection')
        # return luigi.LocalTarget(os.path.join(self.result_path, 'row-wise.jl'))

    def requires(self):
        return NumberFormatNormalization(self.dataset_path, self.result_path)

    def run(self):
        # step 1: obtain all non-hopping aggregation candidates for each row
        # step 2: build search space with conflict rules for each row
        # step 3: construct hierarchy trees
        # step 4: select the correct tree, and filter out incorrect aggregation candidates.

        # step 1: obtain all non-hopping aggregation candidates for each row
        # step 2: prune the aggregation candidates that do not reach the cross-row satisfied threshold
        # step 3: construct hierarchy trees
        # step 4: select the correct tree, and filter out incorrect aggregation candidates.

        with self.input().open('r') as input_file:
            # pool = Pool(cpu_count())
            file_dict_output = []
            files_dict = [json.loads(line) for line in input_file]
            for file_dict in tqdm(files_dict, desc='Row wise sum detection'):
                start_time = time.time()
                # step 1
                # print(file_dict['file_name'])
                # if file_dict['file_name'] != 'C10001':
                #     continue
                file_dict['aggregation_detection_result'] = {}
                for number_format in file_dict['valid_number_formats']:
                    table_value = np.array(file_dict['valid_number_formats'][number_format])

                    # do dynamic error bound
                    error_bound = self.error_bound if self.error_bound >= 0 else detect_error_bound(table_value)

                    # agg_cands = pool.starmap(self.process_row, zip(non_negative_values, range(len(table_value))))
                    file_cells = np.full_like(table_value, fill_value=table_value, dtype=object)
                    for index, value in np.ndenumerate(table_value):
                        file_cells[index] = Cell(CellIndex(index[0], index[1]), value)

                    forests_by_rows = [AggregationRelationForest(row_cells) for row_cells in file_cells]
                    forest_by_row_index = {}
                    for index, forest in enumerate(forests_by_rows):
                        forest_by_row_index[index] = forest
                    collected_results_by_row = {}
                    while True:
                        ar_cands_by_row = [(detect_proximity_aggregation_relations(forest, error_bound, self.error_strategy), forest) for forest in
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

                        ar_cands_by_column_index = prune_conflict_ar_cands(ar_cands_by_row, axis=0)

                        # prune the candidates that do not appear in X% of all rows. X equals to satisfied_vote_ratio
                        # ar_cands_by_row = prune_occasional_ar_cands(ar_cands_by_row, self.satisfied_vote_ratio, axis=0)

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
                        collected_results_by_row[forest] = list(itertools.chain(*[result_dict for result_dict in results_dict]))

                    collected_results = list(itertools.chain(*[results_dict for _, results_dict in collected_results_by_row.items()]))
                    file_dict['aggregation_detection_result'][number_format] = collected_results
                end_time = time.time()
                exec_time = end_time - start_time
                file_dict['exec_time'][self.__class__.__name__] = exec_time
                file_dict_output.append(file_dict)

        with self.output().open('w') as file_writer:
            for result in file_dict_output:
                file_writer.write(json.dumps(result) + '\n')


class SumDetectionColumnWise(luigi.Task):
    dataset_path = luigi.Parameter()
    result_path = luigi.Parameter()
    error_bound = luigi.FloatParameter(default=1)
    satisfied_vote_ratio = luigi.FloatParameter(default=0.5)
    error_strategy = luigi.Parameter(default='ratio')
    use_extend_strategy = luigi.BoolParameter(default=False)

    def output(self):
        return MockTarget('column-wise-detection')
        # return luigi.LocalTarget(os.path.join(self.result_path, 'column-wise.jl'))

    def requires(self):
        return NumberFormatNormalization(self.dataset_path, self.result_path)

    def run(self):
        # step 1: obtain all non-hopping aggregation candidates for each row
        # step 2: build search space with conflict rules for each row
        # step 3: construct hierarchy trees
        # step 4: select the correct tree, and filter out incorrect aggregation candidates.

        # step 1: obtain all non-hopping aggregation candidates for each row
        # step 2: prune the aggregation candidates that do not reach the cross-row satisfied threshold
        # step 3: construct hierarchy trees
        # step 4: select the correct tree, and filter out incorrect aggregation candidates.

        with self.input().open('r') as input_file:
            # pool = Pool(cpu_count())
            file_dict_output = []
            files_dict = [json.loads(line) for line in input_file]
            for file_dict in tqdm(files_dict, desc='Column wise sum detection'):
                start_time = time.time()
                # step 1
                # print(file_dict['file_name'])
                # if file_dict['file_name'] != 'C10014':
                #     continue
                file_dict['aggregation_detection_result'] = {}
                for number_format in file_dict['valid_number_formats']:
                    # table_value = np.array(file_dict['table_array'])
                    table_value = np.array(file_dict['valid_number_formats'][number_format])

                    # do dynamic error bound
                    error_bound = self.error_bound if self.error_bound >= 0 else detect_error_bound(table_value)

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
                        ar_cands_by_column = [(detect_proximity_aggregation_relations(forest, error_bound, self.error_strategy), forest) for forest in
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
                        collected_results_by_column[forest] = list(itertools.chain(*[result_dict for result_dict in results_dict]))

                    collected_results = list(itertools.chain(*[results_dict for _, results_dict in collected_results_by_column.items()]))
                    file_dict['aggregation_detection_result'][number_format] = collected_results
                end_time = time.time()
                exec_time = end_time - start_time
                file_dict['exec_time'][self.__class__.__name__] = exec_time
                file_dict_output.append(file_dict)

        with self.output().open('w') as file_writer:
            for result in file_dict_output:
                file_writer.write(json.dumps(result) + '\n')


class Aggrdet(luigi.Task):
    dataset_path = luigi.Parameter()
    result_path = luigi.Parameter('./temp/')
    error_level = luigi.FloatParameter(default=0)
    satisfied_vote_ratio = luigi.FloatParameter(default=0.5)
    error_strategy = luigi.Parameter(default='ratio')
    use_extend_strategy = luigi.BoolParameter(default=False)
    timeout = luigi.FloatParameter(default=300)

    def output(self):
        return MockTarget('aggrdet')
        # return luigi.LocalTarget(os.path.join(self.result_path, 'number-formatted.jl'))

    def requires(self):
        return {'sum_det_row_wise': SumDetectionRowWise(self.dataset_path, self.result_path, self.error_level, self.satisfied_vote_ratio, self.error_strategy,
                                                        self.use_extend_strategy),
                'sum_det_col_wise': SumDetectionColumnWise(self.dataset_path, self.result_path, self.error_level, self.satisfied_vote_ratio,
                                                           self.error_strategy,
                                                           self.use_extend_strategy)}

    def run(self):
        with self.input()['sum_det_row_wise'].open('r') as file_reader:
            row_wise_json = [json.loads(line) for line in file_reader]
        with self.input()['sum_det_col_wise'].open('r') as file_reader:
            column_wise_json = [json.loads(line) for line in file_reader]

        result_dict = []
        for row_wise, column_wise in tqdm(zip(row_wise_json, column_wise_json), desc='Select number format'):
            start_time = time.time()
            # print(row_wise['file_name'])
            # if row_wise['file_name'] != 'C10003':
            #     continue
            row_wise_results_by_number_format = row_wise['aggregation_detection_result']
            col_wise_results_by_number_format = column_wise['aggregation_detection_result']
            nf_cands = set(row_wise_results_by_number_format.keys())
            nf_cands.update(set(col_wise_results_by_number_format.keys()))
            nf_cands = sorted(list(nf_cands))
            results = []
            # results = {}
            for number_format in nf_cands:
                row_wise_aggrs = row_wise_results_by_number_format[number_format]
                row_ar = set()
                for r in row_wise_aggrs:
                    aggregator = ast.literal_eval(r[0])
                    aggregator = Cell(CellIndex(aggregator[0], aggregator[1]), None)
                    aggregatees = []
                    for e in r[1].values():
                        aggregatees.extend([ast.literal_eval(v) for v in e])
                    aggregatees = [Cell(CellIndex(e[0], e[1]), None) for e in aggregatees]
                    aggregatees.sort()
                    row_ar.add(AggregationRelation(aggregator, tuple(aggregatees), None))
                col_wise_aggrs = col_wise_results_by_number_format[number_format]
                col_ar = set()
                for r in col_wise_aggrs:
                    aggregator = ast.literal_eval(r[0])
                    aggregator = Cell(CellIndex(aggregator[0], aggregator[1]), None)
                    aggregatees = []
                    for e in r[1].values():
                        aggregatees.extend([ast.literal_eval(v) for v in e])
                    aggregatees = [Cell(CellIndex(e[0], e[1]), None) for e in aggregatees]
                    aggregatees.sort()
                    col_ar.add(AggregationRelation(aggregator, tuple(aggregatees), None))
                det_aggrs = row_ar
                det_aggrs.update(col_ar)
                results.append((number_format, det_aggrs))
                # results[number_format] = det_aggrs
            # number_format = max(results, key=lambda x: len(results[x]))
            # print([(k, len(v)) for k, v in results.items()])
            results.sort(key=lambda x: len(x[1]), reverse=True)
            number_format = results[0][0]
            # print([(k, len(v)) for k, v in results])
            # print(number_format)
            file_output_dict = copy(row_wise)
            file_output_dict['detected_number_format'] = number_format
            det_aggrs = []
            # for det_aggr in results[number_format]:
            for det_aggr in results[0][1]:
                if isinstance(det_aggr, AggregationRelation):
                    aees = [[aee.cell_index.row_index, aee.cell_index.column_index] for aee in det_aggr.aggregatees]
                    aor = [det_aggr.aggregator.cell_index.row_index, det_aggr.aggregator.cell_index.column_index]
                    det_aggrs.append({'aggregator_index': aor, 'aggregatee_indices': aees})
            file_output_dict['detected_aggregations'] = det_aggrs
            file_output_dict.pop('aggregation_detection_result', None)
            file_output_dict['number_formatted_values'] = file_output_dict['valid_number_formats'][number_format]
            file_output_dict.pop('valid_number_formats', None)

            end_time = time.time()
            exec_time = end_time - start_time
            r_exec_time = row_wise['exec_time']
            c_exec_time = column_wise['exec_time']
            file_output_dict['exec_time'] = {**r_exec_time, **c_exec_time}
            file_output_dict['exec_time'][self.__class__.__name__] = exec_time
            file_output_dict['parameters'] = {}
            file_output_dict['parameters']['error_level'] = self.error_level
            file_output_dict['parameters']['error_strategy'] = self.error_strategy
            file_output_dict['parameters']['use_extend_strategy'] = self.use_extend_strategy
            file_output_dict['parameters']['timeout'] = self.timeout
            file_output_dict['parameters']['algorithm'] = self.__class__.__name__

            result_dict.append(file_output_dict)

        with self.output().open('w') as file_writer:
            for file_output_dict in result_dict:
                file_writer.write(json.dumps(file_output_dict) + '\n')


# class Aggrdet(luigi.Task):
#     dataset_path = luigi.Parameter()
#     error_bound = luigi.FloatParameter(default=1)
#     satisfied_vote_ratio = luigi.FloatParameter(default=0.5)
#
#     def output(self):
#         pass
#
#     def requires(self):
#         return {'AggdetRowWise': SumDetectionRowWise(self.dataset_path),
#                 'AggdetColumnWise': SumDetectionColumnWise(self.dataset_path)}
#
#     def run(self):
#         pass


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
