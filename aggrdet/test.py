# Created by lan at 2021/3/14
from dataclasses import dataclass


@dataclass(order=True)
class TestCellIndex:
    row_index: int
    column_index: int


if __name__ == '__main__':
    pass