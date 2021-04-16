# Created by lan at 2021/2/2
import itertools
from enum import Enum
from functools import total_ordering
from typing import Tuple


@total_ordering
class CellIndex:
    def __init__(self, row_index, column_index) -> None:
        self.row_index = row_index
        self.column_index = column_index

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, CellIndex):
            return False
        return self.__key() == o.__key()

    def __lt__(self, other) -> bool:
        if not isinstance(other, CellIndex):
            return False
        return self.__key() < other.__key()

    def __hash__(self) -> int:
        return hash(self.__key())

    def __key(self):
        return self.row_index, self.column_index

    def __str__(self) -> str:
        return '(%d, %d)' % (self.row_index, self.column_index)

    def __repr__(self) -> str:
        return self.__str__()


@total_ordering
class Cell:
    def __init__(self, cell_index: CellIndex, value: str) -> None:
        self.cell_index = cell_index
        self.value = value

    def __key(self):
        return self.cell_index, self.value

    def __eq__(self, other) -> bool:
        if not isinstance(other, Cell):
            return False
        return self.__key() == other.__key()

    def __lt__(self, other) -> bool:
        if not isinstance(other, Cell):
            return False
        return self.cell_index < other.cell_index

    def __hash__(self) -> int:
        return hash(self.__key())

    def __str__(self) -> str:
        return '%s->%s' % (self.cell_index, self.value)

    def __repr__(self) -> str:
        return self.__str__()


class Direction(Enum):
    # Forward means up for a column line, or left for a row line
    FORWARD = 1
    # Backward means down for a column line, or right for a row line
    BACKWARD = 2
    UNKNOWN = 3


class AggregationRelation:

    def __init__(self, aggregator: Cell, aggregatees: Tuple[Cell], operator: str, direction: Direction):
        self.aggregator = aggregator
        self.aggregatees = aggregatees
        self.operator = operator
        self.direction = direction

    def __str__(self) -> str:
        return 'Aggregator: %s; Aggregatees: %s; Operator: %s; Direction: %s' \
               % (self.aggregator, str(self.aggregatees), str(self.operator), str(self.direction.value))

    def __repr__(self) -> str:
        return self.__str__()

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, AggregationRelation):
            return False
        return self.__key() == o.__key()

    def __hash__(self) -> int:
        return hash(self.__key())

    def __key(self):
        return self.aggregator, self.aggregatees, self.operator, self.direction


if __name__ == '__main__':
    # a = Cell(CellIndex(4, 1), 15341)
    # b = Cell(CellIndex(4, 1), 15341)
    # a = Direction.FORWARD
    # b = Direction.FORWARD
    # a = (Cell(CellIndex(4, 2), 39945), Cell(CellIndex(4, 3), 24604))
    # b = (Cell(CellIndex(4, 2), 39945), Cell(CellIndex(4, 3), 24604))
    a = AggregationRelation(Cell(CellIndex(4, 1), 15341), (Cell(CellIndex(4, 2), 39945), Cell(CellIndex(4, 3), 24604)), 'Sum', Direction.FORWARD)
    b = AggregationRelation(Cell(CellIndex(4, 1), 15341), (Cell(CellIndex(4, 2), 39945), Cell(CellIndex(4, 3), 24604)), 'Sum', Direction.FORWARD)
    print(a)
    print(b)
    print(a == b)