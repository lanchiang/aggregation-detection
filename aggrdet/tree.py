# Created by lan at 2021/2/8
from copy import copy
from typing import List

from treelib import Tree

from elements import AggregationRelation, Cell, CellIndex, Direction


class AggregationRelationForest:

    def __init__(self, cells: List[Cell]) -> None:
        self.forest = {}
        self.cells = {}
        for index, cell in enumerate(cells):
            tree = Tree()
            tree.create_node(tag=cell.cell_index.__str__(), identifier=cell.cell_index.__str__(), data=cell.value)
            self.cells[cell.cell_index] = Cell(cell.cell_index, cell.value)
            self.forest[cell.cell_index] = [tree]
        self.pool = []

    def consume_relation(self, ar: AggregationRelation) -> None:
        trees_aggor = self.forest[ar.aggregator.cell_index]
        last_tree = trees_aggor[len(trees_aggor) - 1]
        if last_tree.depth() > 0:
            # there is already a grown tree, need to create another one for this new aggregation relation
            tree = Tree()
            tree.create_node(tag=ar.aggregator.cell_index.__str__(), identifier=ar.aggregator.cell_index.__str__(), data=ar.aggregator.value)
            for aee in ar.aggregatees:
                tree.create_node(tag=aee.cell_index.__str__(), identifier=aee.cell_index.__str__(), data=aee.value,
                                 parent=ar.aggregator.cell_index.__str__())
            self.forest[ar.aggregator.cell_index].append(tree)
            self.pool.append(copy(tree))
        else:
            for aee in ar.aggregatees:
                last_tree.create_node(tag=aee.cell_index.__str__(), identifier=aee.cell_index.__str__(), data=aee.value,
                                      parent=ar.aggregator.cell_index.__str__())
            self.pool.append(copy(last_tree))

    def remove_consumed_aggregators(self, ar_cands: List[AggregationRelation]) -> None:
        for ar in ar_cands:
            for aee in ar.aggregatees:
                self.forest.pop(aee.cell_index, None)

    def remove_consumed_aggregator(self, ar_cand: AggregationRelation) -> None:
        for aee in ar_cand.aggregatees:
            self.forest.pop(aee.cell_index, None)

    def remove_consumed_signature(self, signature, axis) -> None:
        # signature is a 2er-tuple, (Aggregator, Aggregatees)
        aggregatee_signatures = signature[1]
        if axis == 0:
            # row wise
            for aggee_signature in aggregatee_signatures:
                self.forest.pop(CellIndex(list(self.forest.keys())[0].row_index, aggee_signature), None)
            pass
        if axis == 1:
            # column wise
            for aggee_signature in aggregatee_signatures:
                cell_index = CellIndex(aggee_signature, list(self.forest.keys())[0].column_index)
                self.forest.pop(cell_index, None)
            pass
        pass

    def is_valid_relation(self, ar: AggregationRelation) -> bool:
        """
        Only using column index, as this is the positional parameter for the ar. Row index is always the same.
        :param ar:
        :return:
        """
        aee_as_root = all([aee.cell_index in self.forest.keys() for aee in ar.aggregatees])
        aor_as_root = ar.aggregator.cell_index in self.forest.keys()
        return True if aee_as_root and aor_as_root else False

    def get_roots(self) -> List[Cell]:
        roots = []
        for cell_index in self.forest.keys():
            cell = self.cells[cell_index]
            roots.append(cell)
        roots.sort()
        return roots

    def results_to_str(self, operator, direction):
        results = []
        for result in self.pool:
            if isinstance(result, Tree):
                result_list = list(result.to_dict(with_data=False).items())
                for r in result_list:
                    results.append((r[0], r[1]['children'], operator, direction))
        return results
