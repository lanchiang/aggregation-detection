# Created by lan at 2021/2/2
import gzip
import json

import numpy as np
from tqdm import tqdm


def load_dataset(path: str):
    with gzip.open(path) as json_file:
        json_dicts = np.array([json.loads(line) for line in json_file])
        dataset = [[jd['file_name'],
                  jd['table_id'],
                  np.array(jd['table_array']),
                  jd['aggregation_annotations'],
                  np.array(['annotations'])
                  ]
                 for jd in tqdm(json_dicts)]
    return dataset


def get_file(dataset, file_name, sheet_name):
    filtered = [d for d in dataset if d[0] == file_name and d[1] == sheet_name]
    return filtered[0] if filtered else None