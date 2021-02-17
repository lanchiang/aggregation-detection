import multiprocessing
from multiprocessing import Pool

import time

import numpy as np

work = (["A", 5], ["B", 2], ["C", 1], ["D", 3])


def work_log(work_data):
    print(" Process %s waiting %s seconds" % (work_data[0], work_data[1]))
    time.sleep(int(work_data[1]))
    print(" Process %s Finished." % work_data[0])
    print()


def process_line(array):
    print(array[0])
    print(multiprocessing.current_process())
    print()
    pass


def pool_handler():
    pool = Pool(4)
    shape = (20, 20)
    l = []
    for i in range(shape[0]):
        l_r = []
        for j in range(shape[1]):
            l_r.append((i, j))
        l.append(l_r)
    array = np.array(l)
    # p.map(work_log, work)
    pool.map(process_line, array)


if __name__ == '__main__':
    pool_handler()
