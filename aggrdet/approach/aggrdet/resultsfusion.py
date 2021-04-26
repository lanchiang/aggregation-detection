# Created by lan at 2021/4/7
import ast
import itertools

from helpers import AggregationDirection, AggregationOperator


def __filter_aggregations_by_signature(results_by_signature):
    # add a mark to the key. It indicates if this entry should be filtered out. A "1" means should be filtered out. All marks are initialized as "0"
    marks = [0 for _ in range(len(results_by_signature))]
    signatures = list(results_by_signature.keys())
    # aggregation_list = list(marked_results_by_signature.values())
    for index, (signature, aggregations) in enumerate(results_by_signature.items()):
        # signature is a 2er-tuple: (Signature, mark)

        # if mark is "1", skip this entry
        if marks[index] == 1:
            continue

        this_aggor = signature[0][0]
        this_aggees = signature[0][1]
        this_operator = signature[1]

        # set the marks of those entries after this, which should be filtered out, as "1"
        if all([e == 1 for e in marks[index + 1: len(results_by_signature)]]):
            break
        for that_index in range(index + 1, len(results_by_signature)):
            that_signature = signatures[that_index]
            if marks[that_index] == 1:
                continue
            that_aggor = that_signature[0][0]
            that_aggees = that_signature[0][1]
            that_operator = that_signature[1]

            # if complete inclusion is satisfied (either way), filter out
            if this_aggor in that_aggees and len(set(this_aggees).intersection(set(that_aggees))) > 0:
                marks[that_index] = 1
            if that_aggor in this_aggees and len(set(this_aggees).intersection(set(that_aggees))) > 0:
                marks[that_index] = 1

            if this_operator == AggregationOperator.DIVISION.value or that_operator == AggregationOperator.DIVISION.value:
                continue

            # if same aggregator, inclusive aggregatees happens (either way), filter out
            if this_aggor == that_aggor and len(set(this_aggees).intersection(set(that_aggees))) > 0:
                marks[that_index] = 1

            # if circular aggregator-aggregatee happens (either way), filter out
            if this_aggor in that_aggees and that_aggor in this_aggees:
                marks[that_index] = 1
    preserved_signatures = [signature for index, signature in enumerate(signatures) if marks[index] == 0]
    preserved_aggregations = [v for k, v in results_by_signature.items() if k in preserved_signatures]
    return list(itertools.chain(*preserved_aggregations))


def fuse_aggregation_results(file_dicts):
    for file_dict in file_dicts:
        file_aggrdet_results = file_dict['aggregation_detection_result']

        for number_format in file_aggrdet_results.keys():
            filtered_results = []
            # row wise fusion
            row_wise_aggr_results = [result for result in file_aggrdet_results[number_format] if result[3] == AggregationDirection.ROW_WISE.value]

            # group by operator and column signature. Column signature of an aggregation is the column index of aggregator and column indices of aggregatees
            result_grp_by_column_sign_operator = {}
            for aggrdet_result in row_wise_aggr_results:
                aggregator_index = ast.literal_eval(aggrdet_result[0])
                aggregatees_indices = [ast.literal_eval(aggregatee_index) for aggregatee_index in aggrdet_result[1]]
                column_signature = (aggregator_index[1], tuple([e[1] for e in aggregatees_indices]))
                operator = aggrdet_result[2]

                signature = (column_signature, operator)
                if signature not in result_grp_by_column_sign_operator:
                    result_grp_by_column_sign_operator[signature] = []
                result_grp_by_column_sign_operator[signature].append(aggrdet_result)

            # order the result groups by 1) the length of aggregatee list; 2) the number of their detected aggregations
            # the higher the group is in the rank, the more detected aggregations in a group and the longer the aggregatee list is
            result_grp_by_column_sign_operator = {k: v for k, v in
                                                  sorted(result_grp_by_column_sign_operator.items(), key=lambda e: (len(e[0][0][1]), len(e[1])), reverse=True)}
            filtered_results_row_wise = __filter_aggregations_by_signature(result_grp_by_column_sign_operator)
            filtered_results.extend(filtered_results_row_wise)

            # column wise fusion
            # aggrdet_result is a 4er-tuple, (Aggregator_index_string, Aggregatee_indices_strings, Operator_type, Aggregation_direction)
            column_wise_aggr_results = [result for result in file_aggrdet_results[number_format] if result[3] == AggregationDirection.COLUMN_WISE.value]

            # group by operator and row signature. Row signature of an aggregation is the row index of aggregator and row indices of aggregatees
            result_grp_by_row_sign_operator = {}
            for aggrdet_result in column_wise_aggr_results:
                aggregator_index = ast.literal_eval(aggrdet_result[0])
                aggregatees_indices = [ast.literal_eval(aggregatee_index) for aggregatee_index in aggrdet_result[1]]
                row_signature = (aggregator_index[0], tuple([e[0] for e in aggregatees_indices]))
                operator = aggrdet_result[2]

                signature = (row_signature, operator)
                if signature not in result_grp_by_row_sign_operator:
                    result_grp_by_row_sign_operator[signature] = []
                result_grp_by_row_sign_operator[signature].append(aggrdet_result)

            # order the result groups by 1) the length of aggregatee list; 2) the number of their detected aggregations
            # the higher the group is in the rank, the more detected aggregations in a group and the longer the aggregatee list is
            result_grp_by_row_sign_operator = {k: v for k, v in
                                               sorted(result_grp_by_row_sign_operator.items(), key=lambda e: (len(e[0][0][1]), len(e[1])), reverse=True)}
            filtered_results_column_wise = __filter_aggregations_by_signature(result_grp_by_row_sign_operator)
            filtered_results.extend(filtered_results_column_wise)

            file_aggrdet_results[number_format] = filtered_results
            pass
    pass
