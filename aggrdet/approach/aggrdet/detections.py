# Created by lan at 2021/3/12
import itertools
import time

import numpy as np

from algorithm import detect_proximity_aggregation_relations, prune_conflict_ar_cands, remove_duplicates
from approach.approach import AggregationDetection
from bruteforce import delayed_bruteforce
from elements import Cell, CellIndex, AggregationRelation
from tree import AggregationRelationForest


class SumDetection(AggregationDetection):

    def detect_row_wise_aggregations(self, file_dict):
        start_time = time.time()
        file_dict['aggregation_detection_result'] = {}
        for number_format in file_dict['valid_number_formats']:

            # Todo: just for fair timeout comparison
            if number_format != file_dict['number_format']:
                continue

            table_value = np.array(file_dict['valid_number_formats'][number_format])

            file_cells = np.full_like(table_value, fill_value=table_value, dtype=object)
            for index, value in np.ndenumerate(table_value):
                file_cells[index] = Cell(CellIndex(index[0], index[1]), value)

            forests_by_rows = [AggregationRelationForest(row_cells) for row_cells in file_cells]
            forest_by_row_index = {}
            for index, forest in enumerate(forests_by_rows):
                forest_by_row_index[index] = forest
            collected_results_by_row = {}
            while True:
                ar_cands_by_row = [(detect_proximity_aggregation_relations(forest, self.error_level, self.error_strategy), forest) for forest in
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
                # collected_results_by_row[forest] = list(itertools.chain(*[result_dict for result_dict in results_dict]))
                collected_results_by_row[forest] = results_dict

            if self.use_delayed_bruteforce:
                delayed_bruteforce(collected_results_by_row, table_value, error_bound, axis=0)
                remove_duplicates(collected_results_by_row)

            collected_results = list(itertools.chain(*[results_dict for _, results_dict in collected_results_by_row.items()]))

            file_dict['aggregation_detection_result'][number_format] = collected_results
        end_time = time.time()
        exec_time = end_time - start_time
        file_dict['exec_time'][self.__class__.__name__] = exec_time
        return file_dict

    def detect_column_wise_aggregations(self, file_dict):
        pass