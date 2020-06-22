from airflow.plugins_manager import AirflowPlugin

from base_data_quality_operator\
    import BaseDataQualityOperator
from data_quality_threshold_check_operator\
    import DataQualityThresholdCheckOperator
from data_quality_threshold_sql_check_operator\
    import DataQualityThresholdSQLCheckOperator


class RaybeamQualityPlugin(AirflowPlugin):
    name = "rb_quality_plugin"
    operators = [
        BaseDataQualityOperator,
        DataQualityThresholdCheckOperator,
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
