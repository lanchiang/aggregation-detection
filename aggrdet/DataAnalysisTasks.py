# Created by lan at 2021/4/20
import json
import math
import os.path
from decimal import Decimal, InvalidOperation

import luigi
import pandas
from tqdm import tqdm

from dataprep import DataPreparation, NumberFormatNormalization


class ExtractAnnotations2CSV(luigi.Task):
    dataset_path = luigi.Parameter()
    result_path = luigi.Parameter(default='./debug/')

    def output(self):
        return luigi.LocalTarget(os.path.join(self.result_path, 'annotations.csv'))

    def requires(self):
        return NumberFormatNormalization(dataset_path=self.dataset_path, result_path=self.result_path, debug=True)

    def run(self):
        with self.input().open('r') as input_reader:
            file_dicts = [json.loads(line) for line in input_reader]

        annotations = []
        for file_dict in tqdm(file_dicts, desc='Extract annotations...'):
            file_id = (file_dict['file_name'], file_dict['table_id'])
            # print(file_id)
            try:
                file_values = file_dict['valid_number_formats'][file_dict['number_format']]
            except KeyError:
                continue

            for annotation in file_dict['aggregation_annotations']:
                aggregator_value = file_values[annotation['aggregator_index'][0]][annotation['aggregator_index'][1]]
                try:
                    magnitude = math.log10(abs(Decimal(aggregator_value)))
                except (ValueError, InvalidOperation):
                    magnitude = Decimal(0)

                aggregatee_values = [file_values[aggregatee_index[0]][aggregatee_index[1]] for aggregatee_index in annotation['aggregatee_indices']]
                annotations.append((file_id[0],
                                    file_id[1],
                                    aggregator_value,
                                    int(magnitude),
                                    annotation['aggregator_index'],
                                    aggregatee_values,
                                    annotation['aggregatee_indices'],
                                    annotation['operator'],
                                    annotation['error_level_percent']
                                    ))

        df = pandas.DataFrame(annotations,
                              columns=['file_name', 'sheet_name', 'aggregator_value', 'aggregator_value_order_of_magnitude', 'aggregator_index',
                                       'aggregatee_values', 'aggregatee_indices',
                                       'operator', 'real_error_level'])
        df.to_csv(self.output().path, index=True, index_label=['id'])


if __name__ == '__main__':
    luigi.run()
