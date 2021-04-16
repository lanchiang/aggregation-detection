# Created by lan at 2021/3/29
import decimal
import json
import math
import os
import time
from copy import copy
from decimal import Decimal

import luigi
import numpy as np
from luigi.mock import MockTarget
from pebble import ProcessPool
from tqdm import tqdm

from approach.aggrdet._MultiAggregateeAggrdet import MultiAggregateeAggrdet
from approach.approach import AggregationDetection
from elements import AggregationRelation, Direction, Cell, CellIndex
from helpers import is_empty_cell, hard_empty_cell_values, AggregationOperator, empty_cell_values
from tree import AggregationRelationForest


class AverageDetection(MultiAggregateeAggrdet):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.operator = AggregationOperator.AVERAGE.value

    def output(self):
        if self.debug:
            return luigi.LocalTarget(os.path.join(self.result_path, 'aggrdet-average-detection.jl'))
        else:
            return MockTarget('aggrdet-average-detection')

    def run(self):
        with self.input().open('r') as file_reader:
            files_dict = [json.loads(line) for line in file_reader]

            files_dict_map = {}
            for file_dict in files_dict:
                file_dict['detected_number_format'] = ''
                file_dict['detected_aggregations'] = []
                file_dict['aggregation_detection_result'] = {file_dict['number_format']: []}
                file_dict['exec_time'][self.__class__.__name__] = math.nan

                files_dict_map[(file_dict['file_name'], file_dict['table_id'])] = file_dict

            print('Detect average aggregations...')

            with ProcessPool(max_workers=self.cpu_count, max_tasks=1) as pool:
                returned_result = pool.map(self.detect_aggregations, files_dict, timeout=self.timeout).result()

            self.process_results(returned_result, files_dict_map, self.__class__.__name__)

        with self.output().open('w') as file_writer:
            for file_output_dict in tqdm(files_dict_map.values(), desc='Serialize average results'):
                file_writer.write(json.dumps(file_output_dict) + '\n')

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
    #         if index == 18:
    #             stop = 0
    #
    #         aggregator_digit_places = 10
    #
    #         # forward
    #         aggregatee_cells = []
    #         expected_sum = Decimal(0)
    #         is_equal = False
    #         current_lowest_error_level = math.inf
    #         for i in range(index + 1, len(roots) + 1):
    #             if i == len(roots):
    #                 if current_lowest_error_level <= error_bound:
    #                     aggregatee_cells.append(roots[i - 1])
    #                     is_equal = True
    #                 break
    #
    #             try:
    #                 aggregatee = Decimal(roots[i].value)
    #             except decimal.InvalidOperation:
    #                 # if is_empty_cell(roots[i].value):
    #                 #     aggregatee_cells.append(roots[i])
    #                 continue
    #             else:
    #                 expected_sum += aggregatee if not aggregatee.is_nan() else Decimal(0.0)
    #                 aggregatee_cells.append(roots[i])
    #                 expected_average = round(expected_sum / Decimal(len(aggregatee_cells)), aggregator_digit_places)
    #                 if error_strategy == 'ratio':
    #                     if aggregator_value == 0:
    #                         latest_error_level = abs(expected_average - aggregator_value)
    #                     else:
    #                         latest_error_level = abs((expected_average - aggregator_value) / aggregator_value)
    #                     if current_lowest_error_level < latest_error_level:
    #                         if current_lowest_error_level <= error_bound:
    #                             is_equal = True
    #                         break
    #                     else:
    #                         current_lowest_error_level = latest_error_level
    #                 elif error_strategy == 'value':
    #                     if abs(expected_average - aggregator_value) <= error_bound:
    #                         is_equal = True
    #                         break
    #         if is_equal and len(aggregatee_cells) > 2:
    #             aggregatee_cells = aggregatee_cells[:len(aggregatee_cells) - 1]
    #             cursor = len(aggregatee_cells)
    #             for i in reversed(range(len(aggregatee_cells))):
    #                 if aggregatee_cells[i].value in hard_empty_cell_values:
    #                     cursor = i
    #                 else:
    #                     break
    #             aggregatee_cells = aggregatee_cells[:cursor]
    #             if len(aggregatee_cells) > 1:
    #                 if not (aggregator_value == 0 and all([aee_cell.value in hard_empty_cell_values or aee_cell.value == 0 for aee_cell in aggregatee_cells])):
    #                     ar = AggregationRelation(copy(root), tuple([copy(aee_cell) for aee_cell in aggregatee_cells]), self.operator, Direction.FORWARD)
    #                     real_error_level = current_lowest_error_level
    #                     aggregation_candidates.append((ar, real_error_level))
    #
    #         # backward
    #         aggregatee_cells = []
    #         expected_sum = Decimal(0.0)
    #         is_equal = False
    #         current_lowest_error_level = math.inf
    #         for i in range(index - 1, -2, -1):
    #             if i < 0:
    #                 if current_lowest_error_level <= error_bound:
    #                     aggregatee_cells.append(roots[0])
    #                     is_equal = True
    #                 break
    #
    #             try:
    #                 aggregatee = Decimal(roots[i].value)
    #             except decimal.InvalidOperation:
    #                 # if is_empty_cell(roots[i].value):
    #                 #     aggregatee_cells.append(roots[i])
    #                 continue
    #             else:
    #                 expected_sum += aggregatee if not aggregatee.is_nan() else Decimal(0.0)
    #                 aggregatee_cells.append(roots[i])
    #                 expected_average = round(expected_sum / Decimal(len(aggregatee_cells)), aggregator_digit_places)
    #                 if error_strategy == 'ratio':
    #                     if aggregator_value == 0:
    #                         latest_error_level = abs(expected_average - aggregator_value)
    #                     else:
    #                         latest_error_level = abs((expected_average - aggregator_value) / aggregator_value)
    #                     if current_lowest_error_level < latest_error_level:
    #                         if current_lowest_error_level <= error_bound:
    #                             is_equal = True
    #                         break
    #                     else:
    #                         current_lowest_error_level = latest_error_level
    #                 elif error_strategy == 'value':
    #                     if abs(expected_average - aggregator_value) <= error_bound:
    #                         is_equal = True
    #                         break
    #         if is_equal and len(aggregatee_cells) > 2:
    #             aggregatee_cells = aggregatee_cells[:len(aggregatee_cells) - 1]
    #             cursor = len(aggregatee_cells)
    #             for i in reversed(range(len(aggregatee_cells))):
    #                 if aggregatee_cells[i].value in hard_empty_cell_values:
    #                     cursor = i
    #                 else:
    #                     break
    #             aggregatee_cells = aggregatee_cells[:cursor]
    #             if len(aggregatee_cells) > 1:
    #                 if not (aggregator_value == 0 and all([aee_cell.value in hard_empty_cell_values or aee_cell.value == 0 for aee_cell in aggregatee_cells])):
    #                     ar = AggregationRelation(copy(root), tuple([copy(aee_cell) for aee_cell in aggregatee_cells]), self.operator, Direction.BACKWARD)
    #                     real_error_level = current_lowest_error_level
    #                     aggregation_candidates.append((ar, real_error_level))
    #
    #     return aggregation_candidates

    def detect_proximity_aggregation_relations(self, forest: AggregationRelationForest, error_bound: float, error_strategy):
        roots = forest.get_roots()
        aggregation_candidates = []
        for index, root in enumerate(roots):
            if root.cell_index == CellIndex(7, 7):
                stop = 0
            try:
                aggregator_value = Decimal(root.value)
            except decimal.InvalidOperation:
                continue
            if aggregator_value.is_nan():
                continue
            if error_strategy == 'refrained' and aggregator_value == 0:
                continue

            aggregator_digit_places = 5

            # forward
            aggregatee_cells = []
            expected_sum = Decimal(0)
            is_equal = False
            latest_error_level = math.inf
            for i in range(index + 1, len(roots)):
                # if i == len(roots):
                #     if current_lowest_error_level <= error_bound:
                #         aggregatee_cells.append(roots[i - 1])
                #         is_equal = True
                #     break

                try:
                    aggregatee = Decimal(roots[i].value)
                except decimal.InvalidOperation:
                    # if is_empty_cell(roots[i].value):
                    #     aggregatee_cells.append(roots[i])
                    continue
                else:
                    expected_sum += aggregatee if not aggregatee.is_nan() else Decimal(0.0)
                    aggregatee_cells.append(roots[i])
                    expected_average = round(expected_sum / Decimal(len(aggregatee_cells)), aggregator_digit_places)
                    # expected_average = expected_sum / Decimal(len(aggregatee_cells))
                    if error_strategy == 'ratio':
                        if aggregator_value == 0:
                            latest_error_level = abs(expected_average - aggregator_value)
                        else:
                            latest_error_level = abs((expected_average - aggregator_value) / aggregator_value)
                        if latest_error_level <= error_bound:
                            # if every aggregatee cell has the same value, treat it not a valid aggregation
                            if len(set([elem.value for elem in aggregatee_cells])) != 1:
                                is_equal = True
                                break
                        # if current_lowest_error_level < latest_error_level:
                        #     if current_lowest_error_level <= error_bound:
                        #         is_equal = True
                        #     break
                        # else:
                        #     current_lowest_error_level = latest_error_level
                    elif error_strategy == 'value':
                        if abs(expected_average - aggregator_value) <= error_bound:
                            is_equal = True
                            break
            if is_equal and len(aggregatee_cells) >= 2:
                # aggregatee_cells = aggregatee_cells[:len(aggregatee_cells) - 1]
                # cursor = len(aggregatee_cells)
                # for i in reversed(range(len(aggregatee_cells))):
                #     if aggregatee_cells[i].value in hard_empty_cell_values:
                #         cursor = i
                #     else:
                #         break
                # aggregatee_cells = aggregatee_cells[:cursor]
                # if len(aggregatee_cells) > 1:
                if not (aggregator_value == 0 and all([aee_cell.value in hard_empty_cell_values or aee_cell.value == 0 for aee_cell in aggregatee_cells])):
                    ar = AggregationRelation(copy(root), tuple([copy(aee_cell) for aee_cell in aggregatee_cells]), self.operator, Direction.FORWARD)
                    real_error_level = latest_error_level
                    aggregation_candidates.append((ar, real_error_level))

            # backward
            aggregatee_cells = []
            expected_sum = Decimal(0.0)
            is_equal = False
            latest_error_level = math.inf
            for i in reversed(range(index)):
                # if i < 0:
                #     if current_lowest_error_level <= error_bound:
                #         aggregatee_cells.append(roots[0])
                #         is_equal = True
                #     break

                try:
                    aggregatee = Decimal(roots[i].value)
                except decimal.InvalidOperation:
                    # if is_empty_cell(roots[i].value):
                    #     aggregatee_cells.append(roots[i])
                    continue
                else:
                    expected_sum += aggregatee if not aggregatee.is_nan() else Decimal(0.0)
                    aggregatee_cells.append(roots[i])
                    expected_average = round(expected_sum / Decimal(len(aggregatee_cells)), aggregator_digit_places)
                    # expected_average = expected_sum / Decimal(len(aggregatee_cells))
                    if error_strategy == 'ratio':
                        if aggregator_value == 0:
                            latest_error_level = abs(expected_average - aggregator_value)
                        else:
                            latest_error_level = abs((expected_average - aggregator_value) / aggregator_value)
                        if latest_error_level <= error_bound:
                            # if every aggregatee cell has the same value, treat it not a valid aggregation
                            if len(set([elem.value for elem in aggregatee_cells])) != 1:
                                is_equal = True
                                break
                        # if current_lowest_error_level < latest_error_level:
                        #     if current_lowest_error_level <= error_bound:
                        #         is_equal = True
                        #     break
                        # else:
                        #     current_lowest_error_level = latest_error_level
                    elif error_strategy == 'value':
                        if abs(expected_average - aggregator_value) <= error_bound:
                            is_equal = True
                            break
            if is_equal and len(aggregatee_cells) >= 2:
                # aggregatee_cells = aggregatee_cells[:len(aggregatee_cells) - 1]
                # cursor = len(aggregatee_cells)
                # for i in reversed(range(len(aggregatee_cells))):
                #     if aggregatee_cells[i].value in hard_empty_cell_values:
                #         cursor = i
                #     else:
                #         break
                # aggregatee_cells = aggregatee_cells[:cursor]
                if len(aggregatee_cells) > 1:
                    if not (aggregator_value == 0 and all([aee_cell.value in hard_empty_cell_values or aee_cell.value == 0 for aee_cell in aggregatee_cells])):
                        ar = AggregationRelation(copy(root), tuple([copy(aee_cell) for aee_cell in aggregatee_cells]), self.operator, Direction.BACKWARD)
                        real_error_level = latest_error_level
                        aggregation_candidates.append((ar, real_error_level))

        return aggregation_candidates

    def average_equal(self, aggregator_value, aggregatees, based_aggregator_value, error_bound):
        digit_places = 5
        expected_average = round(sum(aggregatees) / len(aggregatees), digit_places)
        # expected_average = sum(aggregatees) / len(aggregatees)
        if aggregator_value == 0 or based_aggregator_value == 0:
            error_level = abs(expected_average - aggregator_value)
        else:
            error_level = abs((expected_average - aggregator_value) / based_aggregator_value)
        # print(error_level)
        return error_level if error_level <= error_bound else math.inf

    def mend_adjacent_aggregations(self, ar_cands_by_line, file_content, error_bound, axis):
        mended_ar_cands_by_line = copy(ar_cands_by_line)
        if axis == 0:
            # row wise
            mended_ar_cands_by_line = {elem[0][0][0].aggregator.cell_index.row_index: elem for elem in mended_ar_cands_by_line}
            valid_row_indices = [elem[0][0][0].aggregator.cell_index.row_index for elem in ar_cands_by_line]
            ar_cands_by_column_index_direction = {}
            for ar_cands in ar_cands_by_line:
                for ar_cand in ar_cands[0]:
                    aggregator = ar_cand[0].aggregator
                    column_index_direction = (aggregator.cell_index.column_index, ar_cand[0].direction)
                    if column_index_direction not in ar_cands_by_column_index_direction:
                        ar_cands_by_column_index_direction[column_index_direction] = []
                    ar_cands_by_column_index_direction[column_index_direction].append(ar_cand)
            for key, aggregations in ar_cands_by_column_index_direction.items():
                sorted_aggregations = sorted(aggregations, key=lambda x: len(x[0].aggregatees), reverse=True)
                for aggregation in sorted_aggregations:
                    aggregatees = aggregation[0].aggregatees
                    aggregatee_column_indices = [aggregatee.cell_index.column_index for aggregatee in aggregatees]
                    aggregator = aggregation[0].aggregator
                    for row_index in valid_row_indices:
                        if row_index == aggregation[0].aggregator.cell_index.row_index:
                            continue
                        possible_aggee_values = [file_content[row_index][ci] for ci in aggregatee_column_indices]
                        numberized_aggee_values = [self.to_number(elem, AggregationOperator.AVERAGE.value) for elem in possible_aggee_values]
                        numberized_aggee_values = [value for value in numberized_aggee_values if value is not None]
                        if not bool(numberized_aggee_values):
                            continue

                        possible_aggor_value = self.to_number(file_content[row_index][key[0]], AggregationOperator.AVERAGE.value)
                        based_aggregator_value = self.to_number(aggregator.value, AggregationOperator.AVERAGE.value)
                        if not all([n is not None for n in numberized_aggee_values]) or not possible_aggor_value:
                            continue
                        real_error_level = self.average_equal(possible_aggor_value, numberized_aggee_values, based_aggregator_value, error_bound)
                        if real_error_level != math.inf:
                            mended_aggregation = AggregationRelation(Cell(CellIndex(row_index, key[0]), str(possible_aggor_value)),
                                                                     tuple([Cell(CellIndex(row_index, ci), file_content[row_index][ci]) for ci in
                                                                            aggregatee_column_indices]),
                                                                     AggregationOperator.AVERAGE.value, aggregation[0].direction)
                            mended_collection = [elem[0] for elem in mended_ar_cands_by_line[row_index][0]]
                            if mended_aggregation not in mended_collection:
                                mended_aggregation = AggregationRelation(Cell(CellIndex(row_index, key[0]), str(possible_aggor_value)),
                                                                         tuple([Cell(CellIndex(row_index, ci), file_content[row_index][ci]) for ci in
                                                                                aggregatee_column_indices]),
                                                                         AggregationOperator.AVERAGE.value, aggregation[0].direction)
                                mended_ar_cands_by_line[row_index][0].append((mended_aggregation, real_error_level))
                        pass
        else:
            # column wise
            mended_ar_cands_by_line = {elem[0][0][0].aggregator.cell_index.column_index: elem for elem in mended_ar_cands_by_line}
            valid_column_indices = [elem[0][0][0].aggregator.cell_index.column_index for elem in ar_cands_by_line]
            ar_cands_by_row_index_direction = {}
            for ar_cands in ar_cands_by_line:
                for ar_cand in ar_cands[0]:
                    aggregator = ar_cand[0].aggregator
                    row_index_direction = (aggregator.cell_index.row_index, ar_cand[0].direction)
                    if row_index_direction not in ar_cands_by_row_index_direction:
                        ar_cands_by_row_index_direction[row_index_direction] = []
                    ar_cands_by_row_index_direction[row_index_direction].append(ar_cand)
            for key, aggregations in ar_cands_by_row_index_direction.items():
                sorted_aggregations = sorted(aggregations, key=lambda x: len(x[0].aggregatees), reverse=True)
                for aggregation in sorted_aggregations:
                    aggregatees = aggregation[0].aggregatees
                    aggregatee_row_indices = [aggregatee.cell_index.row_index for aggregatee in aggregatees]
                    aggregator = aggregation[0].aggregator
                    if CellIndex(53, 12) == aggregator.cell_index:
                        pass
                    for column_index in valid_column_indices:
                        if column_index == aggregation[0].aggregator.cell_index.column_index:
                            continue
                        possible_aggee_values = [file_content[ri][column_index] for ri in aggregatee_row_indices]
                        numberized_aggee_values = [self.to_number(elem, AggregationOperator.AVERAGE.value) for elem in possible_aggee_values]
                        numberized_aggee_values = [value for value in numberized_aggee_values if value is not None]
                        if not bool(numberized_aggee_values):
                            continue

                        possible_aggor_value = self.to_number(file_content[key[0]][column_index], AggregationOperator.AVERAGE.value)
                        based_aggregator_value = self.to_number(aggregator.value, AggregationOperator.AVERAGE.value)
                        if not all([n is not None for n in numberized_aggee_values]) or not possible_aggor_value:
                            continue

                        real_error_level = self.average_equal(possible_aggor_value, numberized_aggee_values, based_aggregator_value, error_bound)
                        if real_error_level != math.inf:
                            mended_aggregation = AggregationRelation(Cell(CellIndex(key[0], column_index), str(possible_aggor_value)),
                                                                     tuple([Cell(CellIndex(ri, column_index), file_content[ri][column_index]) for ri in
                                                                            aggregatee_row_indices]),
                                                                     AggregationOperator.AVERAGE.value, aggregation[0].direction)
                            mended_collection = [elem[0] for elem in mended_ar_cands_by_line[column_index][0]]
                            if mended_aggregation not in mended_collection:
                                mended_aggregation = AggregationRelation(Cell(CellIndex(key[0], column_index), str(possible_aggor_value)),
                                                                         tuple([Cell(CellIndex(ri, column_index), file_content[ri][column_index]) for ri in
                                                                                aggregatee_row_indices]),
                                                                         AggregationOperator.AVERAGE.value, aggregation[0].direction)
                                mended_ar_cands_by_line[column_index][0].append((mended_aggregation, real_error_level))
                        pass
                    pass
        return mended_ar_cands_by_line
