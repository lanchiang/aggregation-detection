# Created by lan at 2021/2/2
import ast
import gzip
import itertools
import json
from copy import deepcopy, copy
from multiprocessing import Pool, cpu_count
from multiprocessing import current_process
from typing import List

import luigi
import numpy as np
from tqdm import tqdm

from data import detect_number_format, transform_number_format
from dataprep import DataPreparation
from elements import AggregationRelation, CellIndex, Direction, AggregationRelationCombination, Cell
from helpers import is_empty_cell
from hierarchy import HierarchyForest
from tree import AggregationRelationForest


class NumberFormatNormalization(luigi.Task):
    """
    This task runs the number format normalization task on the initial input file, and output a file whose values are normalized.
    """

    dataset_path = luigi.Parameter()
    sample_ratio = luigi.FloatParameter(default=0.1)

    def output(self):
        return luigi.LocalTarget('temp/file-number-format.jl')

    def requires(self):
        # return LoadDataset(self.dataset_path)
        return DataPreparation(self.dataset_path)

    def run(self):
        with self.input().open('r') as input_file:
            dump_json_string = []
            for line in input_file:
                file_json_dict = json.loads(line)
                file_value_array = np.array(file_json_dict['table_array'])
                # if file_json_dict['file_name'] != 'C10009':
                #     continue
                number_format = detect_number_format(file_value_array)
                # file_json_dict['table_array'] = transform_number_format(file_json_dict['table_array'], number_format)
                tnff = {}
                for nf in number_format:
                    tnff[nf] = transform_number_format(file_json_dict['table_array'], nf)
                file_json_dict['valid_number_formats'] = tnff
                dump_json_string.append(json.dumps(file_json_dict))

        with self.output().open('w') as file_writer:
            for djs in dump_json_string:
                file_writer.write(djs + '\n')


def detect_error_bound(file_value: np.ndarray) -> float:
    numbers = []
    for index, value in np.ndenumerate(file_value):
        try:
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
        for ar_cand in ar_cands:
            aggregator = ar_cand.aggregator
            aggregatees = ar_cand.aggregatees
            if axis == 0:
                ar_tuple = (aggregator.cell_index.column_index, tuple([aggregatee.cell_index.column_index for aggregatee in aggregatees]))
            else:
                ar_tuple = (aggregator.cell_index.row_index, tuple([aggregatee.cell_index.row_index for aggregatee in aggregatees]))
            if ar_tuple not in satisfied_cands_index:
                satisfied_cands_index[ar_tuple] = []
            satisfied_cands_index[ar_tuple].append(ar_cand)
    satisfied_cands_index = {k: v for k, v in reversed(sorted(satisfied_cands_index.items(), key=lambda item: len(item[1])))}

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

    # non_conflict_ar_cands_by_row = []
    # for ar_cands in ar_cands_by_line:
    #     non_conflict_ar_cands = []
    #     for ar_cand in ar_cands:
    #         aggregator = ar_cand.aggregator
    #         aggregatees = ar_cand.aggregatees
    #         if axis == 0:
    #             ar_tuple = (aggregator.cell_index.column_index, tuple([aggregatee.cell_index.column_index for aggregatee in aggregatees]))
    #         else:
    #             ar_tuple = (aggregator.cell_index.row_index, tuple([aggregatee.cell_index.row_index for aggregatee in aggregatees]))
    #         if ar_tuple in non_conflict_ar_cands_index:
    #             non_conflict_ar_cands.append(copy(ar_cand))
    #     non_conflict_ar_cands_by_row.append(non_conflict_ar_cands)
    # return non_conflict_ar_cands_by_row


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


def detect_proximity_aggregation_relations(forest: AggregationRelationForest, error_bound: float) -> list:
    """
    find all proximity aggregation candidates for the given row
    :param forest:
    :return:
    """
    roots = forest.get_roots()
    aggregation_candidates = []
    for index, root in enumerate(roots):
        try:
            value = float(root.value)
        except Exception:
            continue

        # forward
        aggregatee_cells = []
        sum = 0
        is_equal = False
        for i in range(index + 1, len(roots)):
            # if this cell is empty, allows to continue
            if is_empty_cell(roots[i].value):
                aggregatee_cells.append(roots[i])
                continue
            try:
                aggregatee = float(roots[i].value)
            except Exception:
                break
            else:
                sum += aggregatee
                aggregatee_cells.append(roots[i])
                if abs(sum - value) <= error_bound:
                    is_equal = True
                    break
        if is_equal and len(aggregatee_cells) > 1:
            ar = AggregationRelation(copy(root), tuple([copy(aee_cell) for aee_cell in aggregatee_cells]), Direction.FORWARD)
            aggregation_candidates.append(ar)

        # backward
        aggregatee_cells = []
        sum = 0
        is_equal = False
        for i in reversed(range(index)):
            # if this cell is empty, allows to continue
            if is_empty_cell(roots[i].value):
                aggregatee_cells.append(roots[i])
                continue
            try:
                aggregatee = float(roots[i].value)
            except Exception:
                break
            else:
                sum += aggregatee
                aggregatee_cells.append(roots[i])
                if abs(sum - value) <= error_bound:
                    is_equal = True
                    break
        if is_equal and len(aggregatee_cells) > 1:
            ar = AggregationRelation(copy(root), tuple([copy(aee_cell) for aee_cell in aggregatee_cells]), Direction.BACKWARD)
            aggregation_candidates.append(ar)
    return aggregation_candidates


class SumDetectionRowWise(luigi.Task):
    dataset_path = luigi.Parameter()
    error_bound = luigi.FloatParameter(default=1)
    satisfied_vote_ratio = luigi.FloatParameter(default=0.5)

    def requires(self):
        return NumberFormatNormalization(self.dataset_path)

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
            for file_dict in tqdm(files_dict):
                # step 1
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
                    collected_results_by_row = {}
                    while True:
                        ar_cands_by_row = [(detect_proximity_aggregation_relations(forest, error_bound), forest) for forest in
                                           forests_by_rows]
                        # get all non empty ar_cands
                        ar_cands_by_row = list(filter(lambda x: bool(x[0]), ar_cands_by_row))
                        if not ar_cands_by_row:
                            break

                        forest_indexed_by_ar_cand = {}
                        for ar_cands, forest in ar_cands_by_row:
                            for ar_cand in ar_cands:
                                forest_indexed_by_ar_cand[ar_cand] = forest

                        ar_cands_by_row, forests_by_rows = list(zip(*ar_cands_by_row))

                        ar_cands_by_row = prune_conflict_ar_cands(ar_cands_by_row, axis=0)

                        # prune the candidates that do not appear in X% of all rows. X equals to satisfied_vote_ratio
                        # ar_cands_by_row = prune_occasional_ar_cands(ar_cands_by_row, self.satisfied_vote_ratio, axis=0)

                        if not bool(ar_cands_by_row):
                            break

                        for _, ar_cands in ar_cands_by_row.items():
                            for i in range(len(ar_cands)):
                                ar_cands[i] = (ar_cands[i], forest_indexed_by_ar_cand[ar_cands[i]])

                        for _, ar_cands_w_forest in ar_cands_by_row.items():
                            [forest.consume_relation(ar_cand) for ar_cand, forest in ar_cands_w_forest]
                        for _, ar_cands_w_forest in ar_cands_by_row.items():
                            [forest.remove_consumed_aggregator(ar_cand) for ar_cand, forest in ar_cands_w_forest]

                        for forest in forests_by_rows:
                            results_dict = forest.results_to_str('Sum')
                            collected_results_by_row[forest] = list(itertools.chain(*[result_dict for result_dict in results_dict]))
                        # for row_index, ar_cands_w_forest in ar_cands_by_row.items():
                        #     results_dict = [forest.results_to_str('Sum') for _, forest in ar_cands_w_forest]
                        #     [collected_results_by_row.extend(result_dict) for result_dict in results_dict]

                        # for ar_cands, forest in zip(ar_cands_by_row, forests_by_rows):
                        #     for ar_cand in ar_cands:
                        #         forest.consume_relation(ar_cand)
                        #     forest.remove_consumed_aggregators(ar_cands)
                        #
                        # for forest in forests_by_rows:
                        #     results_dict = forest.results_to_str('Sum')
                        #     for result_dict in results_dict:
                        #         collected_results_by_row.extend(result_dict)
                    collected_results = list(itertools.chain(*[results_dict for _, results_dict in collected_results_by_row.items()]))
                    file_dict['aggregation_detection_result'][number_format] = collected_results
                file_dict_output.append(file_dict)

        with self.output().open('w') as file_writer:
            for result in file_dict_output:
                file_writer.write(json.dumps(result) + '\n')

    def output(self):
        return luigi.LocalTarget('temp/row-wise.jl')


class SumDetectionColumnWise(luigi.Task):
    dataset_path = luigi.Parameter()
    error_bound = luigi.FloatParameter(default=1)
    satisfied_vote_ratio = luigi.FloatParameter(default=0.5)

    def requires(self):
        return NumberFormatNormalization(self.dataset_path)

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
            for file_dict in tqdm(files_dict):
                # step 1
                # print(file_dict['file_name'])
                # if file_dict['file_name'] != 'C10001':
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
                    collected_results_by_column = {}
                    while True:
                        ar_cands_by_column = [(detect_proximity_aggregation_relations(forest, error_bound), forest) for forest in
                                              forests_by_columns]
                        # get all non empty ar_cands
                        ar_cands_by_column = list(filter(lambda x: bool(x[0]), ar_cands_by_column))
                        if not ar_cands_by_column:
                            break

                        forest_indexed_by_ar_cand = {}
                        for ar_cands, forest in ar_cands_by_column:
                            for ar_cand in ar_cands:
                                forest_indexed_by_ar_cand[ar_cand] = forest

                        ar_cands_by_column, forests_by_columns = list(zip(*ar_cands_by_column))

                        ar_cands_by_column = prune_conflict_ar_cands(ar_cands_by_column, axis=1)
                        # prune the candidates that do not appear in X% of all rows. X equals to satisfied_vote_ratio
                        # ar_cands_by_column = prune_occasional_ar_cands(ar_cands_by_column, self.satisfied_vote_ratio, axis=1)
                        if not bool(ar_cands_by_column):
                            break

                        for _, ar_cands in ar_cands_by_column.items():
                            for i in range(len(ar_cands)):
                                ar_cands[i] = (ar_cands[i], forest_indexed_by_ar_cand[ar_cands[i]])

                        # for ar_cands, forest in zip(ar_cands_by_column, forests_by_columns):
                        #     for ar_cand in ar_cands:
                        #         forest.consume_relation(ar_cand)
                        #     forest.remove_consumed_aggregators(ar_cands)
                        for row_index, ar_cands_w_forest in ar_cands_by_column.items():
                            [forest.consume_relation(ar_cand) for ar_cand, forest in ar_cands_w_forest]
                        for row_index, ar_cands_w_forest in ar_cands_by_column.items():
                            [forest.remove_consumed_aggregator(ar_cand) for ar_cand, forest in ar_cands_w_forest]

                        for forest in forests_by_columns:
                            results_dict = forest.results_to_str('Sum')
                            collected_results_by_column[forest] = list(itertools.chain(*[result_dict for result_dict in results_dict]))

                        # for row_index, ar_cands_w_forest in ar_cands_by_column.items():
                        #     results_dict = [forest.results_to_str('Sum') for _, forest in ar_cands_w_forest]
                        #     [collected_results_by_column.extend(result_dict) for result_dict in results_dict]

                        # for forest in forests_by_columns:
                        #     results_dict = forest.results_to_str('Sum')
                        #     # results_dict = forest.results_to_list('Sum')
                        #     for result_dict in results_dict:
                        #         collected_results_by_column.extend(result_dict)
                    collected_results = list(itertools.chain(*[results_dict for _, results_dict in collected_results_by_column.items()]))
                    file_dict['aggregation_detection_result'][number_format] = collected_results
                file_dict_output.append(file_dict)

        with self.output().open('w') as file_writer:
            for result in file_dict_output:
                file_writer.write(json.dumps(result) + '\n')

    def output(self):
        return luigi.LocalTarget('temp/column-wise.jl')


class SelectNumberFormat(luigi.Task):
    dataset_path = luigi.Parameter()
    error_bound = luigi.FloatParameter(default=1)
    satisfied_vote_ratio = luigi.FloatParameter(default=0.5)

    def output(self):
        return luigi.LocalTarget('temp/number-formatted.jl')

    def requires(self):
        return {'sum_det_row_wise': SumDetectionRowWise(self.dataset_path, self.error_bound, self.satisfied_vote_ratio),
                'sum_det_col_wise': SumDetectionColumnWise(self.dataset_path, self.error_bound, self.satisfied_vote_ratio)}

    def run(self):
        with self.input()['sum_det_row_wise'].open('r') as file_reader:
            row_wise_json = [json.loads(line) for line in file_reader]
        with self.input()['sum_det_col_wise'].open('r') as file_reader:
            column_wise_json = [json.loads(line) for line in file_reader]

        result_dict = []
        for row_wise, column_wise in zip(row_wise_json, column_wise_json):
            # print(row_wise['file_name'])
            # if row_wise['file_name'] != 'C10004':
            #     continue
            row_wise_results_by_number_format = row_wise['aggregation_detection_result']
            col_wise_results_by_number_format = column_wise['aggregation_detection_result']
            nf_cands = set(row_wise_results_by_number_format.keys())
            nf_cands.update(set(col_wise_results_by_number_format.keys()))
            results = {}
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
                results[number_format] = det_aggrs
            number_format = max(results, key=lambda x: len(results[x]))
            file_output_dict = copy(row_wise)
            file_output_dict['detected_number_format'] = number_format
            det_aggrs = []
            for det_aggr in results[number_format]:
                if isinstance(det_aggr, AggregationRelation):
                    aees = [[aee.cell_index.row_index, aee.cell_index.column_index] for aee in det_aggr.aggregatees]
                    aor = [det_aggr.aggregator.cell_index.row_index, det_aggr.aggregator.cell_index.column_index]
                    det_aggrs.append({'aggregator_index': aor, 'aggregatee_indices': aees})
            file_output_dict['detected_aggregations'] = det_aggrs
            file_output_dict.pop('aggregation_detection_result', None)
            file_output_dict['number_formatted_values'] = file_output_dict['valid_number_formats'][number_format]
            file_output_dict.pop('valid_number_formats', None)
            result_dict.append(file_output_dict)

        with self.output().open('w') as file_writer:
            for file_output_dict in result_dict:
                file_writer.write(json.dumps(file_output_dict) + '\n')


class Aggrdet(luigi.Task):
    dataset_path = luigi.Parameter()

    def output(self):
        pass

    def requires(self):
        return {'AggdetRowWise': SumDetectionRowWise(self.dataset_path),
                'AggdetColumnWise': SumDetectionColumnWise(self.dataset_path)}

    def run(self):
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


def build_aggr_relation_search_space(ar_cands: List[AggregationRelation]) -> set:
    ar_valid_combi = []
    ar_ss_recursive(ar_cands, AggregationRelationCombination(), ar_valid_combi)
    # need to permute each ar_ss set, as subsets of each of them are not guaranteed to be created in the recursion
    ar_ss = set()
    for ss in ar_valid_combi:
        for combi in ss.all_combinations():
            ar_ss.add(combi)
        # ar_ss.extend(ss.all_combinations())
    return ar_ss


def ar_ss_recursive(allowed_ar_cands: List[AggregationRelation], ar_combi: AggregationRelationCombination,
                    ar_combi_ss: List[AggregationRelationCombination]):
    if not allowed_ar_cands:
        ar_combi_ss.append(deepcopy(ar_combi))
        return
    for index, ar_cand in enumerate(allowed_ar_cands):
        ar_combi.add_ar(ar_cand)
        compatible_ar_cands = select_compatible_ar_cands(ar_cand, allowed_ar_cands[index + 1:])
        ar_ss_recursive(compatible_ar_cands, ar_combi, ar_combi_ss)
        ar_combi.remove_tail()


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
