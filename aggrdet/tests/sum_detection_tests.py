# Created by lan at 2021/2/5
import unittest

from algorithm import build_aggr_relation_search_space
from elements import AggregationRelation, CellIndex, Direction


class TestSumDetectionMethod(unittest.TestCase):

    def setUp(self) -> None:
        pass

    def test_build_aggregation_relation_ss(self):
        ars = [AggregationRelation(
            CellIndex(3, 4),
            (CellIndex(3, 5), CellIndex(3, 6), CellIndex(3, 7)),
            Direction.FORWARD
        ), AggregationRelation(
            CellIndex(3, 7),
            (CellIndex(3, 8), CellIndex(3, 9)),
            Direction.FORWARD
        ), AggregationRelation(
            CellIndex(3, 5),
            (CellIndex(3, 2), CellIndex(3, 3), CellIndex(3, 4)),
            Direction.BACKWARD
        ), AggregationRelation(
            CellIndex(3, 5),
            (CellIndex(3, 6), CellIndex(3, 7), CellIndex(3, 8)),
            Direction.FORWARD
        ), AggregationRelation(
            CellIndex(3, 4),
            (CellIndex(3, 2), CellIndex(3, 3)),
            Direction.BACKWARD
        )]
        candidates = build_aggr_relation_search_space(ars)
        return
