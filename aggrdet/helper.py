# Created by lan at 2021/3/9
import os

import yaml

from definitions import ROOT_DIR


def load_database_config():
    """
    A helper function to load database configuration entries to memory.

    :return: host, database, user, password, port.
    """
    with open(os.path.join(ROOT_DIR, '../config.yaml'), 'r') as config_file_reader:
        conn_config = yaml.safe_load(config_file_reader)
    host = conn_config['instances'][0]['host']
    database = conn_config['instances'][0]['dbname']
    user = conn_config['instances'][0]['username']
    password = conn_config['instances'][0]['password']
    port = conn_config['instances'][0]['port']
    return host, database, user, password, port
