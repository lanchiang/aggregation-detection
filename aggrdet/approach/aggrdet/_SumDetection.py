# Created by lan at 2021/3/25
import decimal
import math
import os
from copy import copy, deepcopy
from decimal import Decimal

import luigi
from luigi.mock import MockTarget

from approach.BottomUpDetection import BottomUpAggregationDetection
from approach.approach import AggregationDetection
from elements import Cell, CellIndex, AggregationRelation, Direction
from helpers import hard_empty_cell_values, AggregationOperator
from tree import AggregationRelationForest


class SumDetection(BottomUpAggregationDetection):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.operator = AggregationOperator.SUM.value
        self.task_name = self.__class__.__name__
        self.error_level = self.error_level_dict[self.operator] if self.operator in self.error_level_dict else self.error_level

    def output(self):
        if self.debug:
            return luigi.LocalTarget(os.path.join(self.result_path, 'aggrdet-sum-detection.jl'))
        else:
            return MockTarget('aggrdet-sum-detection')

    def detect_proximity_aggregation_relations(self, forest: AggregationRelationForest, error_bound: float, error_strategy):
        roots = forest.get_roots()
        aggregation_candidates = []
        for index, root in enumerate(roots):
            if root.cell_index == CellIndex(14, 1):
                stop = 0
            try:
                aggregator_value = Decimal(root.value)
            except decimal.InvalidOperation:
                continue
            if aggregator_value.is_nan():
                continue
            if error_strategy == 'refrained' and aggregator_value == 0:
                continue

            # adjusted_error_bound = self.adjust_error_bound(aggregator_value, error_bound)
            adjusted_error_bound = error_bound

            # forward
            aggregatee_cells = []
            expected_sum = Decimal(0.0)
            is_equal = False
            latest_error_level = math.inf
            for i in range(index + 1, len(roots)):
                # if i == len(roots):
                #     if current_lowest_error_level <= error_bound:
                #         aggregatee_cells.append(roots[i - 1])
                #         is_equal = True
                #     break

                # if this cell is empty, allows to continue
                try:
                    aggregatee = Decimal(roots[i].value)
                except decimal.InvalidOperation:
                    # if is_empty_cell(roots[i].value):
                    #     aggregatee_cells.append(roots[i])
                    continue
                else:
                    # Todo: this approrach dismisses the impact of negative numbers in aggregatee list, may cause aggregatee list shorter than should be.
                    expected_sum += aggregatee if not aggregatee.is_nan() else Decimal(0.0)
                    aggregatee_cells.append(roots[i])
                    if error_strategy == 'ratio':
                        if aggregator_value == 0:
                            latest_error_level = abs(expected_sum - aggregator_value)
                        else:
                            latest_error_level = abs((expected_sum - aggregator_value) / aggregator_value)
                        if latest_error_level <= adjusted_error_bound:
                            is_equal = True
                            break

                    elif error_strategy == 'value':
                        if abs(expected_sum - aggregator_value) <= adjusted_error_bound:
                            is_equal = True
                            break
            if is_equal and len(aggregatee_cells) >= 2:
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

                # if this cell is empty, allows to continue
                # if is_empty_cell(roots[i].value):
                #     aggregatee_cells.append(roots[i])
                #     continue
                try:
                    aggregatee = Decimal(roots[i].value)
                except decimal.InvalidOperation:
                    # if is_empty_cell(roots[i].value):
                    #     aggregatee_cells.append(roots[i])
                    continue
                else:
                    expected_sum += aggregatee if not aggregatee.is_nan() else Decimal(0.0)
                    aggregatee_cells.append(roots[i])
                    if error_strategy == 'ratio':
                        if aggregator_value == 0:
                            latest_error_level = abs(expected_sum - aggregator_value)
                        else:
                            latest_error_level = abs((expected_sum - aggregator_value) / aggregator_value)
                        if latest_error_level <= adjusted_error_bound:
                            is_equal = True
                            break
                    elif error_strategy == 'value':
                        if abs(expected_sum - aggregator_value) <= adjusted_error_bound:
                            is_equal = True
                            break
            if is_equal and len(aggregatee_cells) >= 2:
                if not (aggregator_value == 0 and all([aee_cell.value in hard_empty_cell_values or aee_cell.value == 0 for aee_cell in aggregatee_cells])):
                    ar = AggregationRelation(copy(root), tuple([copy(aee_cell) for aee_cell in aggregatee_cells]), self.operator, Direction.BACKWARD)
                    real_error_level = latest_error_level
                    aggregation_candidates.append((ar, real_error_level))
        return aggregation_candidates

    def is_equal(self, aggregator_value, aggregatees, based_aggregator_value, error_bound):
        # adjusted_error_bound = self.adjust_error_bound(aggregator_value, error_bound)
        adjusted_error_bound = error_bound
        expected_sum = sum(aggregatees)
        if aggregator_value == 0 or based_aggregator_value == 0:
            error_level = abs(expected_sum - aggregator_value)
        else:
            error_level = abs((expected_sum - aggregator_value) / based_aggregator_value)
        return error_level if error_level <= adjusted_error_bound else math.inf

    def mend_adjacent_aggregations(self, ar_cands_by_line, file_content, error_bound, axis):
        # non_empty_ar_cands_by_line = list(filter(lambda item: bool(item[1][0]), ar_cands_by_line.items()))
        non_empty_ar_cands_by_line = {k: v for k, v in ar_cands_by_line.items() if bool(v[0])}
        if axis == 0:
            # row wise
            # valid_row_indices = [elem[0][0][0].aggregator.cell_index.row_index for elem in non_empty_ar_cands_by_line]
            valid_row_indices = ar_cands_by_line.keys()
            ar_cands_by_column_index_direction = {}
            for ar_cands in non_empty_ar_cands_by_line.values():
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
                        # if (row_index, key[0]) == (222, 3):
                        #     print('STOP')
                        possible_aggee_values = [file_content[row_index][ci] for ci in aggregatee_column_indices]
                        numberized_aggee_values = [self.to_number(elem, AggregationOperator.SUM.value) for elem in possible_aggee_values]
                        numberized_aggee_values = [value if value is not None else Decimal(0) for value in numberized_aggee_values]
                        possible_aggor_value = self.to_number(file_content[row_index][key[0]], AggregationOperator.SUM.value)
                        based_aggregator_value = self.to_number(aggregator.value, AggregationOperator.SUM.value)
                        if not all([n is not None for n in numberized_aggee_values]) or not possible_aggor_value:
                            continue
                        if possible_aggor_value.is_nan() or any([e.is_nan() for e in numberized_aggee_values]):
                            continue
                        real_error_level = self.is_equal(possible_aggor_value, numberized_aggee_values, possible_aggor_value, error_bound)
                        if real_error_level != math.inf:
                            mended_aggregation = AggregationRelation(Cell(CellIndex(row_index, key[0]), str(possible_aggor_value)),
                                                                     tuple([Cell(CellIndex(row_index, ci), file_content[row_index][ci]) for ci in
                                                                            aggregatee_column_indices]),
                                                                     AggregationOperator.SUM.value, aggregation[0].direction)
                            mended_collection = [elem[0] for elem in ar_cands_by_line[row_index][0]]
                            if mended_aggregation not in mended_collection:
                                mended_aggregation = AggregationRelation(Cell(CellIndex(row_index, key[0]), str(possible_aggor_value)),
                                                                         tuple([Cell(CellIndex(row_index, ci), file_content[row_index][ci]) for ci in
                                                                                aggregatee_column_indices]),
                                                                         AggregationOperator.SUM.value, aggregation[0].direction)
                                ar_cands_by_line[row_index][0].append((mended_aggregation, real_error_level))
        else:
            # column wise
            # valid_column_indices = [elem[0][0][0].aggregator.cell_index.column_index for elem in ar_cands_by_line]
            valid_column_indices = ar_cands_by_line.keys()
            ar_cands_by_row_index_direction = {}
            for ar_cands in non_empty_ar_cands_by_line.values():
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
                    for column_index in valid_column_indices:
                        if column_index == aggregation[0].aggregator.cell_index.column_index:
                            continue
                        possible_aggee_values = [file_content[ri][column_index] for ri in aggregatee_row_indices]
                        numberized_aggee_values = [self.to_number(elem, AggregationOperator.SUM.value) for elem in possible_aggee_values]
                        numberized_aggee_values = [value if value is not None else Decimal(0) for value in numberized_aggee_values]
                        possible_aggor_value = self.to_number(file_content[key[0]][column_index], AggregationOperator.SUM.value)
                        based_aggregator_value = self.to_number(aggregator.value, AggregationOperator.SUM.value)
                        if not all([n is not None for n in numberized_aggee_values]) or not possible_aggor_value:
                            continue
                        if possible_aggor_value.is_nan() or any([e.is_nan() for e in numberized_aggee_values]):
                            continue
                        real_error_level = self.is_equal(possible_aggor_value, numberized_aggee_values, possible_aggor_value, error_bound)
                        if real_error_level != math.inf:
                            mended_aggregation = AggregationRelation(Cell(CellIndex(key[0], column_index), str(possible_aggor_value)),
                                                                     tuple([Cell(CellIndex(ri, column_index), file_content[ri][column_index]) for ri in
                                                                            aggregatee_row_indices]),
                                                                     AggregationOperator.SUM.value, aggregation[0].direction)
                            mended_collection = [elem[0] for elem in ar_cands_by_line[column_index][0]]
                            if mended_aggregation not in mended_collection:
                                mended_aggregation = AggregationRelation(Cell(CellIndex(key[0], column_index), str(possible_aggor_value)),
                                                                         tuple([Cell(CellIndex(ri, column_index), file_content[ri][column_index]) for ri in
                                                                                aggregatee_row_indices]),
                                                                         AggregationOperator.SUM.value, aggregation[0].direction)
                                ar_cands_by_line[column_index][0].append((mended_aggregation, real_error_level))
        pass

    def generate_ar_candidates_similar_headers(self):

        pass
