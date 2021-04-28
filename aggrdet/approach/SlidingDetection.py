# Created by lan at 2021/4/21
import itertools
import time
from abc import ABC
from copy import deepcopy
from pprint import pprint

import numpy as np

from approach.IndividualAggregationDetectionTask import IndividualAggregationDetection
from approach.aggrdet.detections import prune_conflict_ar_cands
from approach.approach import AggregationDetection
from elements import Cell, CellIndex, AggregationRelation
from helpers import AggregationDirection
from tree import AggregationRelationForest


class SlidingAggregationDetection(IndividualAggregationDetection, ABC):

    # Specify how many neighboring cells (in the same rows/columns) must be considered when looking for the aggregatees of an aggregator candidate
    PROXIMITY_WINDOW_SIZE = 10

    def detect_row_wise_aggregations(self, file_dict):
        start_time = time.time()
        _file_dict = deepcopy(file_dict)
        _file_dict['aggregation_detection_result'] = {}
        for number_format in _file_dict['valid_number_formats']:

            # Todo: just for fair timeout comparison
            if number_format != _file_dict['number_format']:
                continue

            table_value = np.array(_file_dict['valid_number_formats'][number_format])
            numeric_line_indices = _file_dict['numeric_line_indices'][number_format]

            file_cells = np.full_like(table_value, fill_value=table_value, dtype=object)
            for index, value in np.ndenumerate(table_value):
                file_cells[index] = Cell(CellIndex(index[0], index[1]), value)

            forests_by_rows = [AggregationRelationForest(row_cells) for row_cells in file_cells]
            forest_by_row_index = {}
            for index, forest in enumerate(forests_by_rows):
                forest_by_row_index[index] = forest
            ar_cands_by_row = [(self.detect_proximity_aggregation_relations(forest, self.error_level, 'ratio'), forest) for forest in
                               forests_by_rows]
            # get all non empty ar_cands
            ar_cands_by_row = list(filter(lambda x: bool(x[0]), ar_cands_by_row))
            if not ar_cands_by_row:
                collected_results = []
            else:
                self.mend_adjacent_aggregations(ar_cands_by_row, table_value, self.error_level, axis=0)

                forest_indexed_by_ar_cand = {}
                for ar_cands, forest in ar_cands_by_row:
                    for ar_cand in ar_cands:
                        forest_indexed_by_ar_cand[ar_cand[0]] = forest

                ar_cands_by_row, forests_by_rows = list(zip(*ar_cands_by_row))

                ar_cands_by_column_index = prune_conflict_ar_cands(ar_cands_by_row, axis=0)

                ar_cands_by_column_index = {k: v for k, v in ar_cands_by_column_index.items() if
                                            len(v) / len(numeric_line_indices[1][str(k[0])]) >= self.NUMERIC_SATISFIED_RATIO}

                collected_results = []
                if bool(ar_cands_by_column_index):
                    for _, ar_cands in ar_cands_by_column_index.items():
                        for ar_cand in ar_cands:
                            aggregator = str(ar_cand.aggregator.cell_index)
                            aggregatees = [str(aggregatee.cell_index) for aggregatee in ar_cand.aggregatees]
                            operator = ar_cand.operator
                            collected_results.append((aggregator, aggregatees, operator, AggregationDirection.ROW_WISE.value))

            _file_dict['aggregation_detection_result'][number_format] = collected_results
        end_time = time.time()
        exec_time = end_time - start_time
        _file_dict['exec_time']['RowWiseDetection'] = exec_time
        return _file_dict

    def detect_column_wise_aggregations(self, file_dict):
        start_time = time.time()
        _file_dict = deepcopy(file_dict)
        _file_dict['aggregation_detection_result'] = {}
        for number_format in _file_dict['valid_number_formats']:
            # Todo: just for fair timeout comparison
            if number_format != _file_dict['number_format']:
                continue

            table_value = np.array(_file_dict['valid_number_formats'][number_format])
            numeric_line_indices = _file_dict['numeric_line_indices'][number_format]

            file_cells = np.full_like(table_value, fill_value=table_value, dtype=object)
            for index, value in np.ndenumerate(table_value):
                file_cells[index] = Cell(CellIndex(index[0], index[1]), value)

            forests_by_columns = [AggregationRelationForest(file_cells[:, i]) for i in range(file_cells.shape[1])]
            forest_by_column_index = {}
            for index, forest in enumerate(forests_by_columns):
                forest_by_column_index[index] = forest
            collected_results_by_column = {}

            ar_cands_by_column = [(self.detect_proximity_aggregation_relations(forest, self.error_level, 'ratio'), forest) for forest in
                                  forests_by_columns]
            # get all non empty ar_cands
            ar_cands_by_column = list(filter(lambda x: bool(x[0]), ar_cands_by_column))
            if not ar_cands_by_column:
                collected_results = []
            else:
                self.mend_adjacent_aggregations(ar_cands_by_column, table_value, self.error_level, axis=1)

                forest_indexed_by_ar_cand = {}
                for ar_cands, forest in ar_cands_by_column:
                    for ar_cand in ar_cands:
                        forest_indexed_by_ar_cand[ar_cand[0]] = forest

                ar_cands_by_column, forests_by_columns = list(zip(*ar_cands_by_column))

                ar_cands_by_row_index = prune_conflict_ar_cands(ar_cands_by_column, axis=1)

                ar_cands_by_row_index = {k: v for k, v in ar_cands_by_row_index.items() if
                                         len(v) / len(numeric_line_indices[0][str(k[0])]) >= self.NUMERIC_SATISFIED_RATIO}

                if not bool(ar_cands_by_row_index):
                    collected_results = []
                else:
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
                                extended_ar_cand = AggregationRelation(extended_ar_cand_aggor, extended_ar_cand_aggees, self.operator,
                                                                       extended_ar_cand_direction)
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

                    for signature in ar_cands_by_row_index.keys():
                        # [forest.remove_consumed_aggregator(ar_cand) for ar_cand, forest in ar_cands_w_forest]
                        [forest.remove_consumed_signature(signature, axis=1) for forest in forest_by_column_index.values()]
                    if self.use_extend_strategy:
                        [forest.remove_consumed_aggregator(ar_cand) for ar_cand, forest in extended_ar_cands_w_forest]

                    for _, forest in forest_by_column_index.items():
                        results_dict = forest.results_to_str(self.operator, AggregationDirection.COLUMN_WISE.value)
                        collected_results_by_column[forest] = results_dict

                    collected_results = list(itertools.chain(*[results_dict for _, results_dict in collected_results_by_column.items()]))

            _file_dict['aggregation_detection_result'][number_format] = collected_results
        end_time = time.time()
        exec_time = end_time - start_time
        _file_dict['exec_time']['ColumnWiseDetection'] = exec_time
        return _file_dict
