# Created by lan at 2021/2/22
import json
from logging import log

import luigi
import numpy as np
import psycopg2
import psycopg2.extensions
from tqdm import tqdm

from dataprep import DataPreparation
from helpers import load_database_config


class UploadDatasetDB(luigi.Task):

    dataset_path = luigi.Parameter()
    dataset_name = luigi.Parameter()
    result_path = luigi.Parameter(default='/debug/')

    def complete(self):
        return False

    def requires(self):
        return DataPreparation(self.dataset_path, self.result_path)

    def run(self):
        with self.input().open('r') as file_reader:
            json_file_dicts = np.array([json.loads(line) for line in file_reader])
        upload_dataset_db(ds_name=self.dataset_name, json_file_dicts=json_file_dicts)


def upload_dataset_db(ds_name, json_file_dicts):
    """
    Upload the dataset to the fact tables (dataset, file, aggregation) in the database.

    :param ds_name: dataset name
    :param json_file_dicts: list of dicts each of which represents the properties of a single file in the dataset
    """
    host, database, user, password, port = load_database_config()
    with psycopg2.connect(dbname=database, user=user, host=host, password=password, port=port) as conn:
        if not isinstance(conn, psycopg2.extensions.connection):
            raise RuntimeError('Postgresql connection initialization failed.')
        with conn.cursor() as curs:
            if not isinstance(curs, psycopg2.extensions.cursor):
                raise RuntimeError('Postgresql cursor initialization failed.')

            # retrieve all operator types
            query = "select * from aggregation_type;"
            curs.execute(query)
            rows = curs.fetchall()
            operator_type = {}
            for row in rows:
                operator_type[row[1]] = row[0]

            # insert an entry to the dataset table
            query = "select exists(select 1 from dataset where name = '%s');" % ds_name
            curs.execute(query)
            rows = curs.fetchall()

            if [(False,)] == rows:
                query = "insert into dataset(id, name) values (default, '%s') returning id;" % ds_name
                curs.execute(query)
                dataset_id = curs.fetchone()[0]
            else:
                query = "select id from dataset where name = '%s'" % ds_name
                curs.execute(query)
                dataset_id = curs.fetchone()[0]

            query = "insert into file(id, dataset_id, file_name, sheet_name, number_format, content) values (default, %s, %s, %s, %s, %s) returning id;"
            insert_anno_query = "insert into aggregation(id, file_id, aggregator, aggregatees, operator, error_level) values (default, %s, Row(%s, %s)::cell_index, %s::cell_index[], %s, %s);"
            for file_dict in tqdm(json_file_dicts, desc='Upload dataset to DB'):
                file_name = file_dict['file_name']
                sheet_name = file_dict['table_id']
                number_format = file_dict['number_format']
                content = file_dict['table_array']
                aggregation_annotations = file_dict['aggregation_annotations']
                q = curs.mogrify(query, [dataset_id, file_name, sheet_name, number_format, content])
                curs.execute(q)

                file_id = curs.fetchone()[0]

                for annotation in aggregation_annotations:
                    aggregator = annotation['aggregator_index']
                    aggregatees = [tuple(e) for e in annotation['aggregatee_indices']]
                    operator = annotation['operator']
                    error_level = annotation['error_bound']
                    q = curs.mogrify(insert_anno_query, [file_id, aggregator[0], aggregator[1], aggregatees, operator_type[operator], error_level])
                    curs.execute(q)
            conn.commit()
        # curs.close()
    # conn.close()


def store_experiment_result(exp_results, ds_name, eval_only_aggor):
    """
    Store experiment results in database.

    :param exp_results: experiment results. A list of dicts, each dict includes various properties of a single file
    :param ds_name: dataset name
    """
    host, database, user, password, port = load_database_config()
    with psycopg2.connect(dbname=database, user=user, host=host, password=password, port=port) as conn:
        if not isinstance(conn, psycopg2.extensions.connection):
            raise RuntimeError('Postgresql connection initialization failed.')
        with conn.cursor() as curs:
            if not isinstance(curs, psycopg2.extensions.cursor):
                raise RuntimeError('Postgresql cursor initialization failed.')

            if len(exp_results) == 0:
                # log(level=Warning, msg='No results returned.')
                print('No results returned.')
                # exit(1)
                return

            algorithm = exp_results[0]['parameters']['algorithm']
            error_level = exp_results[0]['parameters']['error_level']
            error_strategy = exp_results[0]['parameters']['error_strategy']
            extended_strategy = exp_results[0]['parameters']['use_extend_strategy']
            use_delayed_bruteforce_strategy = exp_results[0]['parameters']['use_delayed_bruteforce_strategy']
            timeout = exp_results[0]['parameters']['timeout']

            if not eval_only_aggor:
                results = [(len(result['correct']), len(result['incorrect']), len(result['false_positive']),
                            sum([et for et in result['exec_time'].values()])) for result in exp_results]
            else:
                results = [(len(result['tp_only_aggor']), len(result['fn_only_aggor']), len(result['fp_only_aggor']),
                            sum([et for et in result['exec_time'].values()])) for result in exp_results]

            true_positives = sum([r[0] for r in results])
            false_negatives = sum([r[1] for r in results])
            false_positives = sum([r[2] for r in results])
            precision = true_positives / (true_positives + false_positives)
            recall = true_positives / (true_positives + false_negatives)
            f1 = 2 * precision * recall / (precision + recall)

            exec_time = sum([r[3] for r in results])

            query = 'insert into experiment(algorithm, dataset_id, error_level, only_aggregator, error_strategy, extended_strategy, ' \
                    'delayed_bruteforce_strategy, timeout, precision, recall, f1, exec_time) ' \
                    'values (%s, (select id from dataset where name = %s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) returning id;'
            q = curs.mogrify(query,
                             [algorithm, ds_name, error_level, eval_only_aggor, error_strategy, extended_strategy, use_delayed_bruteforce_strategy, timeout, precision,
                              recall, f1, exec_time])
            # q = curs.mogrify(query, [algorithm, 'troy', error_level, error_strategy, extended_strategy, use_delayed_bruteforce_strategy, timeout, precision, recall, f1, exec_time])
            curs.execute(q)
            experiment_id = curs.fetchone()[0]

            query = 'insert into prediction(experiment_id, file_id, tp_count, fn_count, fp_count, exec_time, true_positives, false_negatives, false_positives) ' \
                    'values (%s, (select id from file where file_name = %s and sheet_name = %s), %s, %s, %s, %s, %s, %s, %s)'
            inserted_list = []
            for result in exp_results:
                exec_time = sum(result['exec_time'].values())
                if not eval_only_aggor:
                    inserted_list.append([experiment_id, result['file_name'], result['table_id'],
                                          len(result['correct']), len(result['incorrect']), len(result['false_positive']), exec_time,
                                          json.dumps(result['correct']),
                                          json.dumps(result['incorrect']),
                                          json.dumps(result['false_positive'])])
                else:
                    inserted_list.append([experiment_id, result['file_name'], result['table_id'],
                                          len(result['tp_only_aggor']), len(result['fn_only_aggor']), len(result['fp_only_aggor']), exec_time,
                                          json.dumps(result['tp_only_aggor']),
                                          json.dumps(result['fn_only_aggor']),
                                          json.dumps(result['fp_only_aggor'])])
            curs.executemany(query, inserted_list)

if __name__ == '__main__':
    luigi.run()
