# Created by lan at 2021/3/12
import ast
import itertools
from copy import copy

from helpers import AggregationOperator


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
    satisfied_cands_index = {k: [e[0] for e in v] for k, v in satisfied_cands_index.items() if len(v) > 1}

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
                    if combination_partial_signature not in results_by_column_combinations:
                        results_by_column_combinations[combination_partial_signature] = []
                    results_by_column_combinations[combination_partial_signature].append(aggregation)
        non_occasional_aggregations = [aggregations for _, aggregations in results_by_column_combinations.items() if len(aggregations) > 1]
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
