# Created by lan at 2021/3/14
import os
from abc import ABC, abstractmethod

import luigi

from dataprep import NumberFormatNormalization


class Approach(ABC):

    @abstractmethod
    def detect_row_wise_aggregations(self, file_dict):
        pass

    @abstractmethod
    def detect_column_wise_aggregations(self, file_dict):
        pass


class AggregationDetection(luigi.Task, Approach):
    dataset_path = luigi.Parameter()
    result_path = luigi.Parameter(default='/debug/')
    error_level = luigi.FloatParameter(default=0)
    use_extend_strategy = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    use_delayed_bruteforce = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    timeout = luigi.FloatParameter(default=300)
    debug = luigi.BoolParameter(default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING)

    def requires(self):
        return NumberFormatNormalization(self.dataset_path, self.result_path, debug=self.debug)


class BruteForce(Approach):
    pass
