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