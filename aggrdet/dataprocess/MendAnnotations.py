# Created by lan at 2021/4/23
import gzip
import json
import os

import luigi
from tqdm import tqdm

from dataprep import NumberFormatNormalization
from helpers import empty_cell_values


class RemoveSpuriousAnnotations(luigi.Task):
    dataset_path = luigi.Parameter()
    result_path = luigi.Parameter(default='./processed/')

    def output(self):
        return luigi.LocalTarget(os.path.join(self.result_path, 'dataset.jl'))

    def requires(self):
        return NumberFormatNormalization(dataset_path=self.dataset_path, result_path=self.result_path, debug=True)

    def run(self):
        with self.input().open('r') as input_reader:
            file_dicts = [json.loads(line) for line in input_reader]

        remove_spurious_annotations(file_dicts)

        with self.output().open('w') as file_writer:
            for file_dict in file_dicts:
                file_writer.write(json.dumps(file_dict) + '\n')


def remove_spurious_annotations(file_dicts):
    for file_dict in file_dicts:

        file_values = file_dict['table_array']

        annotations = []
        for annotation in file_dict['aggregation_annotations']:
            aggregator_value = file_values[annotation['aggregator_index'][0]][annotation['aggregator_index'][1]]

            if aggregator_value in empty_cell_values:
                continue
            annotations.append(annotation)
        file_dict['aggregation_annotations'] = annotations


if __name__ == '__main__':
    input_path = '../../data/dataset.jl.gz'

    with gzip.open(input_path, 'r') as input_reader:
        file_dicts = [json.loads(line) for line in input_reader]

    remove_spurious_annotations(file_dicts)

    output_path = './dataset.jl.gz'

    with gzip.open(output_path, 'wt') as file_writer:
        for file_dict in file_dicts:
            file_writer.write(json.dumps(file_dict) + '\n')