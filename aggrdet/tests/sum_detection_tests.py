# Created by lan at 2021/2/5
import unittest

from algorithm import SumDetectionRowWise
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
        sdrw = SumDetectionRowWise(dataset_path='../data/troy.jl.gz')
        candidates = sdrw.build_aggr_relation_search_space(ars)
        return

    def test_prune_occasional_ar_cands(self):
        ar_cands_by_row = [
            [AggregationRelation(
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
            )],
            [AggregationRelation(
                CellIndex(2, 4),
                (CellIndex(2, 5), CellIndex(2, 6), CellIndex(2, 7)),
                Direction.FORWARD
            ), AggregationRelation(
                CellIndex(2, 7),
                (CellIndex(2, 8), CellIndex(2, 9)),
                Direction.FORWARD
            ), AggregationRelation(
                CellIndex(2, 5),
                (CellIndex(2, 6), CellIndex(2, 7)),
                Direction.FORWARD
            ), AggregationRelation(
                CellIndex(2,4),
                (CellIndex(2, 1), CellIndex(2, 2), CellIndex(2, 3)),
                Direction.BACKWARD
            )]
        ]
        sdrw = SumDetectionRowWise(dataset_path='../data/troy.jl.gz')
        sdrw.prune_occasional_ar_cands(ar_cands_by_row=ar_cands_by_row, satisfied_ratio=0.6)
