from airflow.utils.decorators import apply_defaults

from plugins.base_data_quality_operator import BaseDataQualityOperator

class DataQualityThresholdCheckOperator(BaseDataQualityOperator):

    @apply_defaults
    def __init__(self,
                 min_threshold,
                 max_threshold,
                 eval_threshold=False,
                 threshold_conn_type=None,
                 threshold_conn_id=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.eval_threshold = eval_threshold
        self.min_threshold = min_threshold
        self.max_threshold = max_threshold
        self.threshold_conn_type = threshold_conn_type
        self.threshold_conn_id = threshold_conn_id
    
    @property
    def threshold_conn_type(self):
        return self._threshold_conn_type
    
    @property
    def min_threshold(self):
        return self._min_threshold
    
    @property
    def max_threshold(self):
        return self._max_threshold

    @threshold_conn_type.setter
    def threshold_conn_type(self, conn):
        if self.eval_threshold:
            conn_types = {"postgres", "mysql", "hive"}
            if conn not in conn_types:
                raise ValueError(f"""Connection type "{conn}" is not supported""")
        self._threshold_conn_type = conn
    
    @min_threshold.setter
    def min_threshold(self, sql):
        if self.eval_threshold:
            keywords = {'INSERT INTO', 'UPDATE', 'DROP', 'ALTER'}
            for kw in keywords:
                if kw in sql.upper():
                    raise Exception(f"""Cannot use keyword {kw} in a sql query""")
        self._min_threshold = sql
    
    @max_threshold.setter
    def max_threshold(self, sql):
        if self.eval_threshold:
            keywords = {'INSERT INTO', 'UPDATE', 'DROP', 'ALTER'}
            for kw in keywords:
                if kw in sql.upper():
                    raise Exception(f"""Cannot use keyword {kw} in a sql query""")
        self._max_threshold = sql