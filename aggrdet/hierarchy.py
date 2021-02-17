# Created by lan at 2021/2/7
from treelib import Tree

from elements import AggregationRelation, CellIndex


class HierarchyForest:

    def __init__(self, values) -> None:
        self.forest = []
        self.values = values
        for i, value in enumerate(values):
            single_node_tree = Tree()
            single_node_tree.create_node(i, identifier=i, data=value)
            # single_node_tree.get_node()
            self.forest.append(single_node_tree)

    def add_relation(self, aggregation_relation: AggregationRelation):
        root_tags = [tree.get_node(tree.root).tag for tree in self.forest]
        if aggregation_relation.aggregator.column_index not in root_tags or any([aggregatee.column_index not in root_tags for aggregatee in aggregation_relation.aggregatees]):
            print('Invalid input')
            return

        involved_positions = [aggregatee.column_index for aggregatee in aggregation_relation.aggregatees]
        involved_positions.append(aggregation_relation.aggregator.column_index)

        forest_involved_removed = [tree for tree in self.forest if tree.get_node(tree.root).tag not in involved_positions]
        tree = Tree()
        tree.create_node(aggregation_relation.aggregator.column_index, identifier=aggregation_relation.aggregator.column_index,
                         data=self.values[aggregation_relation.aggregator.column_index])
        for aggregatee in aggregation_relation.aggregatees:
            tree.create_node(aggregatee.column_index, data=self.values[aggregatee.column_index], parent=aggregation_relation.aggregator.column_index)
        forest_involved_removed.append(tree)
        self.forest = forest_involved_removed

    def is_root_of(self, aggregatee: CellIndex) -> list:
        is_root = [tree for tree in self.forest if tree.root == aggregatee.__str__()]
        return is_root


if __name__ == '__main__':
    forest = HierarchyForest(['Hello', 'World', 'A', 'B', 'C'])
    [tree.show(line_type="ascii-em") for tree in forest.forest]
    forest.add_relation(AggregationRelation(
        CellIndex(3, 1),
        (CellIndex(3, 2), CellIndex(3, 3)),
        None
    ))
    print()
    [tree.show(line_type="ascii-em") for tree in forest.forest]
