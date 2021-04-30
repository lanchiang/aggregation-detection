# Created by lan at 2021/4/29
import itertools
import os.path
from copy import copy
from decimal import Decimal

import luigi
from cacheout import FIFOCache
from luigi.mock import MockTarget

from approach.SlidingDetection import SlidingAggregationDetection
from elements import AggregationRelation, Direction
from helpers import AggregationOperator, hard_empty_cell_values
from number import str2decimal
from tree import AggregationRelationForest


class SubtractionDetection(SlidingAggregationDetection):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.operator = AggregationOperator.SUBTRACT.value
        self.task_name = self.__class__.__name__
        self.error_level = self.error_level_dict[self.operator] if self.operator in self.error_level_dict else self.error_level

    def output(self):
        if self.debug:
            return luigi.LocalTarget(os.path.join(self.result_path, 'aggrdet-subtraction-detection.jl'))
        else:
            return MockTarget('aggrdet-subtraction-detection')

    def is_equal(self, aggregator_value, aggregatees, based_aggregator_value, error_bound):
        pass

    def detect_proximity_aggregation_relations(self, forest: AggregationRelationForest, error_bound: float, error_strategy):
        roots = forest.get_roots()
        aggregation_candidates = []
        valid_roots = [root for root in roots if str2decimal(root.value, default=None) is not None]

        subtraction_result_cache = FIFOCache(maxsize=1024)

        for index, root in enumerate(valid_roots):
            aggregator_value = Decimal(root.value)
            if error_strategy == 'refrained' or aggregator_value == 0:
                continue

            # forward
            forward_proximity = [valid_roots[i] for i in range(index + 1, index + 1 + self.PROXIMITY_WINDOW_SIZE) if i < len(valid_roots)]
            for permutation in itertools.permutations(forward_proximity, 2):
                first_element = permutation[0]
                second_element = permutation[1]

                cache_key = (first_element, second_element)
                if cache_key in subtraction_result_cache:
                    expected_value = subtraction_result_cache.get(cache_key)
                else:
                    first_element_decimal_value = str2decimal(first_element.value, default=None)
                    second_element_decimal_value = str2decimal(second_element.value, default=None)
                    aggregatee_values = [first_element_decimal_value, second_element_decimal_value]
                    if aggregator_value == 0 and all([aee_cell.value in hard_empty_cell_values or aee_cell.value == 0 for aee_cell in aggregatee_values]):
                        continue

                    expected_value = first_element_decimal_value - second_element_decimal_value
                    subtraction_result_cache.add(cache_key, expected_value)

                if error_strategy == 'ratio':
                    if aggregator_value == Decimal(0):
                        real_error_level = abs(aggregator_value - expected_value)
                    else:
                        real_error_level = abs((aggregator_value - expected_value) / aggregator_value)
                elif error_strategy == 'value':
                    real_error_level = abs(aggregator_value - expected_value)
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
                if cache_key in subtraction_result_cache:
                    expected_value = subtraction_result_cache.get(cache_key)
                else:
                    first_element_decimal_value = str2decimal(first_element.value, default=None)
                    second_element_decimal_value = str2decimal(second_element.value, default=None)
                    aggregatee_values = [first_element_decimal_value, second_element_decimal_value]
                    if aggregator_value == 0 and all([aee_cell.value in hard_empty_cell_values or aee_cell.value == 0 for aee_cell in aggregatee_values]):
                        continue

                    expected_value = first_element_decimal_value - second_element_decimal_value
                    subtraction_result_cache.add(cache_key, expected_value)

                if error_strategy == 'ratio':
                    if aggregator_value == Decimal(0):
                        real_error_level = abs(aggregator_value - expected_value)
                    else:
                        real_error_level = abs((aggregator_value - expected_value) / aggregator_value)
                elif error_strategy == 'value':
                    real_error_level = abs(aggregator_value - expected_value)
                else:
                    raise NotImplementedError('Other error strategy (%s) has not been implemented yet.' % error_strategy)

                if real_error_level <= error_bound:
                    ar = AggregationRelation(copy(root), tuple(copy(permutation)), self.operator, Direction.DIRECTIONLESS)
                    aggregation_candidates.append((ar, real_error_level))
        return aggregation_candidates

    def mend_adjacent_aggregations(self, ar_cands_by_line, file_content, error_bound, axis):
        pass

    def generate_ar_candidates_similar_headers(self):
        pass
