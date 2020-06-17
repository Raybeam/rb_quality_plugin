from datetime import datetime
from unittest import mock
import pytest

import airflow
from airflow import AirflowException
from airflow.utils.state import State
from rb_quality_plugin.operators.message_writer \
    import MessageWriter

#create table
#create messagewriter with table as parameter
#messagewriter.send_message()
#check table value