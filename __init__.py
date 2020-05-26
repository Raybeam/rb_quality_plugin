from airflow.plugins_manager import AirflowPlugin

from rb_quality_plugin.operators.base_data_quality_operator import BaseDataQualityOperator
from rb_quality_plugin.operators.data_quality_threshold_check_operator import DataQualityThresholdCheckOperator
from rb_quality_plugin.operators.data_quality_threshold_sql_check_operator import DataQualityThresholdSQLCheckOperator


class RbQualityPlugin(AirflowPlugin):
    name = "rb_quality_plugin"
    operators = [BaseDataQualityOperator,
                 DataQualityThresholdSQLCheckOperator,
                 DataQualityThresholdSQLCheckOperator]
    sensors = []
    flask_blueprints = []
    hooks = []
    executors = []
    macros = []
    admin_views = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
