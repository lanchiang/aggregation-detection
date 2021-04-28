# Created by lan at 2021/4/19
import decimal
import itertools
import math
import os.path
from copy import copy
from decimal import Decimal

import luigi
from cacheout import FIFOCache
from luigi.mock import MockTarget

from approach.SlidingDetection import SlidingAggregationDetection
from approach.approach import AggregationDetection
from elements import AggregationRelation, Cell, CellIndex, Direction
from helpers import AggregationOperator
from number import str2decimal
from tree import AggregationRelationForest


class RelativeChangeDetection(SlidingAggregationDetection):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.operator = AggregationOperator.RELATIVE_CHANGE.value
        self.task_name = self.__class__.__name__
        self.error_level = self.error_level_dict[self.operator] if self.operator in self.error_level_dict else self.error_level

    def output(self):
        if self.debug:
            return luigi.LocalTarget(os.path.join(self.result_path, 'aggrdet-relative-change-detection.jl'))
        else:
            return MockTarget('aggrdet-relative-change-detection')

    def detect_proximity_aggregation_relations(self, forest: AggregationRelationForest, error_bound: float, error_strategy):
        roots = forest.get_roots()
        aggregation_candidates = []
        valid_roots = [root for root in roots if str2decimal(root.value, default=None) is not None]

        division_result_cache = FIFOCache(maxsize=1024)

        for index, root in enumerate(valid_roots):
            aggregator_value = Decimal(root.value)
            if error_strategy == 'refrained' or aggregator_value == 0:
                continue
            abs_aggregator_value = abs(aggregator_value)
            if abs_aggregator_value > 100:
                continue
            # if root.cell_index == CellIndex(11, 1):
            #     print('STOP')

            # forward
            forward_proximity = [valid_roots[i] for i in range(index + 1, index + 1 + self.PROXIMITY_WINDOW_SIZE) if i < len(valid_roots)]
            for permutation in itertools.permutations(forward_proximity, 2):
                first_element = permutation[0]
                second_element = permutation[1]
                cache_key = (first_element, second_element)
                if cache_key in division_result_cache:
                    expected_value = division_result_cache.get(cache_key)
                else:
                    first_element_decimal_value = str2decimal(first_element.value, default=None)
                    second_element_decimal_value = str2decimal(second_element.value, default=None)
                    if first_element_decimal_value == 0 or second_element_decimal_value == 0:
                        continue

                    try:
                        expected_value = (second_element_decimal_value - first_element_decimal_value) / first_element_decimal_value
                    except (decimal.DivisionByZero, decimal.InvalidOperation):
                        continue
                    division_result_cache.add(cache_key, expected_value)

                if error_strategy == 'ratio':
                    if aggregator_value == Decimal(0):
                        real_error_level = min(abs(aggregator_value - expected_value), abs(aggregator_value / 100 - expected_value))
                    else:
                        if 1 < aggregator_value <= 100:
                            real_error_level = abs((aggregator_value / 100 - expected_value) / (aggregator_value / 100))
                        else:
                            real_error_level = min(abs((aggregator_value - expected_value) / aggregator_value),
                                                   abs((aggregator_value / 100 - expected_value) / (aggregator_value / 100)))
                elif error_strategy == 'value':
                    real_error_level = min(abs(aggregator_value - expected_value), abs(aggregator_value / 100 - expected_value))
                else:
                    raise NotImplementedError('Other error strategy (%s) has not been implemented yet.' % error_strategy)

                if real_error_level <= error_bound:
                    ar = AggregationRelation(copy(root), tuple(copy(permutation)), self.operator, Direction.DIRECTIONLESS)
                    aggregation_candidates.append((ar, real_error_level))

            # backward
            backward_proximity = [valid_roots[i] for i in range(index - self.PROXIMITY_WINDOW_SIZE, index) if i >= 0]
            for permutation in itertools.permutations(backward_proximity, 2):
                first_element = permutation[0]
                second_element = permutation[1]
                cache_key = (first_element, second_element)
                if cache_key in division_result_cache:
                    expected_value = division_result_cache.get(cache_key)
                else:
                    first_element_decimal_value = str2decimal(first_element.value, default=None)
                    second_element_decimal_value = str2decimal(second_element.value, default=None)
                    if first_element_decimal_value == 0 or second_element_decimal_value == 0:
                        continue

                    try:
                        expected_value = (second_element_decimal_value - first_element_decimal_value) / first_element_decimal_value
                    except (decimal.DivisionByZero, decimal.InvalidOperation):
                        continue
                    division_result_cache.add(cache_key, expected_value)

                if error_strategy == 'ratio':
                    if aggregator_value == Decimal(0):
                        real_error_level = min(abs(aggregator_value - expected_value), abs(aggregator_value / 100 - expected_value))
                    else:
                        if 1 < aggregator_value <= 100:
                            real_error_level = abs((aggregator_value / 100 - expected_value) / (aggregator_value / 100))
                        else:
                            real_error_level = min(abs((aggregator_value - expected_value) / aggregator_value),
                                                   abs((aggregator_value / 100 - expected_value) / (aggregator_value / 100)))
                elif error_strategy == 'value':
                    real_error_level = min(abs(aggregator_value - expected_value), abs(aggregator_value / 100 - expected_value))
                else:
                    raise NotImplementedError('Other error strategy (%s) has not been implemented yet.' % error_strategy)

                if real_error_level <= error_bound:
                    ar = AggregationRelation(copy(root), tuple(copy(permutation)), self.operator, Direction.DIRECTIONLESS)
                    aggregation_candidates.append((ar, real_error_level))

        return aggregation_candidates

    def mend_adjacent_aggregations(self, ar_cands_by_line, file_content, error_bound, axis):
        pass

    def is_equal(self, aggregator_value, aggregatees, based_aggregator_value, error_bound):
        # expected_value = round(aggregatees[0] / aggregatees[1], ndigits=self.DIGIT_PLACES)
        expected_value = abs((aggregatees[1] - aggregatees[0]) / aggregatees[0])
        if aggregator_value == 0 or based_aggregator_value == 0:
            error_level = abs(expected_value - aggregator_value)
        else:
            error_level = abs((expected_value - aggregator_value) / based_aggregator_value)
        return error_level if error_level <= error_bound else math.inf

    def generate_ar_candidates_similar_headers(self):
        pass

