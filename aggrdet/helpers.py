# Created by lan at 2021/1/2

empty_cell_values = ['', '-', 'n/a', 'null', '.', '..', '...', 'x', 'X', '#']

hard_empty_cell_values = ['']

def is_empty_cell(value: str) -> bool:
    is_empty_cell_vector = [ecv == value.lower() for ecv in empty_cell_values]
    return any(is_empty_cell_vector)
