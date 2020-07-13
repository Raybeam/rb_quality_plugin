# Raybeam data quality plugin : rb_quality_plugin 

## Introduction
An Airflow module that includes operators to perform data quality checks. This module includes an Airflow operator that periodically check the results of a query and validates it against a specified threshold.  

Optionally: 

 - If it exceeds the specified threshold level and an Airflow operator that sends an alert through email about given threshold breach
 - Results can be pushed to an external database to store the execution results history

## Quick Setup
Clone a sample airflow workspace (if you dont have an existing airflow repository).  
```
git clone https://github.com/Raybeam/rb_test_airflow/ sample_workspace
cd sample_workspace
```
Clone deploy script into local workspace  
```
git clone https://github.com/Raybeam/rb_plugin_deploy plugins/rb_plugin_deploy
```
Run plugin's deploy script.  

```
./plugins/rb_plugin_deploy/deploy.sh
```
  
## Features
This Airflow module contains the following operators:
- `BaseDataQualityOperator`
- `DataQualityThresholdCheckOperator`
- `DataQualityThresholdCheckSQLOperator`

### BaseDataQualityOperator
`BaseDataQualityOperator` found in [`base_data_quality_operator.py`](plugins/base_data_quality_operator.py) is derived from `BaseOperator` and is used as an inherited class in the other operators. This operator shares common attributes with the operators. These functions include:
- `get_sql_value()` - Given database connection requirements, method will evaluate a result of a sql query and return if and only if is a single column, single row result.
- `send_failure_notification()` - If emails are provided for the task, method will send an email with specifications of the data quality test run when it fails.
- `push()` - **(Optional)** A user-defined method that allows ability to export metadata from a data quality check to an external database for logging.

### DataQualityThresholdCheckOperator
`DataQualityThresholdCheckOperator` found in [`data_quality_threshold_check_operator.py`](plugins/data_quality_threshold_check_operator.py) inherits from `BaseDataQualityOperator`. It is used to perform the data quality check against a threshold range. 

In this operator, the data quality check executes using `get_sql_value()`. Thresholds are given as numeric values and the test will determine if the data quality result is within that range. If configured, `push()` will then be called to send metadata and test results to an external datastore. If the result is within the threshold range, the task passes and the metadata is returned. Otherwise if the result is outside the thresholds, `send_failure_notification()` is called to log the failed test and email users if necessary.

`DataQualityThresholdCheckOperator` also has the functionality for single threshold boundaries. For example, a test can be created to check whether the result of a dq check is larger than the `min_threshold` or smaller than the `max_threshold`.

#### Usage
An example of this operator looks like this:
```python
task = DataQualityThresholdCheckOperator(
    task_id="task_check_average_value",
    sql="SELECT AVG(value) FROM Price;",
    conn_id="postgres_connection",
    min_threshold=20,
    max_threshold=50,
    push_conn_id="push_conn",
    check_description="test to determine whether the average of the Price table is between 20 and 50",
    dag=dag
)
```
The parameters used are:
- `sql` - data quality check sql statement
- `conn_id` - connection id of the location where the sql statement should execute
- `min_threshold` and `max_threshold` - the range in which the data quality result should lie
- `push_conn_id` - (optional) connection id of external table that logs data quality results
- `check_description` - (optional) description text of the test being run

### DataQualityThresholdCheckSQLOperator
`DataQualityThresholdCheckSQLOperator` found in [`data_quality_threshold_sql_check_operator.py`](plugins/data_quality_threshold_sql_check_operator.py) inherits from `BaseDataQualityOperator`. It is almost identical to `DataQualityThresholdCheckOperator`, however the only difference is that instead of passing threshold values into the operator, the operator will take in sql statements for the threshold.

The operator will collect the sql statement for the min and max threshold and before performing the data quality check, it will evaluate these sql statements using `get_sql_value()`. After collecting these threshold values, the operator will evaluate the data quality test and check against the thresholds and determine if the result lies outside or inside the threshold. If the result is within the threshold range, the task passes and the metadata is returned. Otherwise if the result is outside the thresholds, `send_failure_notification()` is called to log the failed test and email users if necessary.

Similarly, `DataQualityThresholdSQLCheckOperator` also has the functionality for single threshold boundaries. For example, a test can be created to check whether the result of a dq check is larger than the `min_threshold_sql` or smaller than the `max_threshold_sql`.

#### Usage
An example of this operator looks like this:
```python
task = DataQualityThresholdCheckOperator(
    task_id="task_check_average_value",
    sql="SELECT AVG(value) FROM Price;",
    conn_id="postgres_connection",
    threshold_conn_id="threshold_postgres_id",
    min_threshold_sql="SELECT MIN(price) FROM Sales WHERE date>=(NOW() - interval '1 month');",
    max_threshold_sql="Select MAX(price) FROM Sales WHERE date>=(NOW() - interval '1 month');",
    push_conn_id="push_conn",
    check_description="test to of whether the average of Price table is between low and high of Sales table from the last month",
    dag=dag
)
```
The parameters used are:
- `sql` - data quality check sql statement
- `conn_id` - connection id of the location where the sql statement should execute
- `threshold_conn_id` - connection id of the location where the threshold sql statements should execute
- `min_threshold_sql` and `max_threshold_sql` - sql statements to define the range in which the data quality result should lie
- `push_conn_id` - (optional) connection id of external table that logs data quality results
- `check_description` - (optional) description text of the test being run


### YAML Usage 
Both operators have the capability to read in YAML configurations by passing the path as the parameter `config_path`. Examples of configuration files can be found in [`example_dags/yaml_dq_check_dag/yaml_configs/`](/example_dags/yaml_dq_check_dag/yaml_configs/). 

With the implementation of configuration file input, [`dq_check_tools.py`](example_dags/utilities/dq_check_tools.py) utilizes this function to dynamically add dq operators to a DAG. There are two methods in this tool: 
1. `create_dq_checks_from_directory()` - Collects all dq checks in a directory, with the option to recurse through sub-directories.
2. `create_dq_checks_from_list()` - Collects all dq checks config files passed through by a list of YAML paths.

### Examples
Example DAG usages are also provided in this package located in the [`example_dags/`](example_dags/) directory. This directory includes examples for both types of Threshold Check Operators and the YAML data quality check tools.

## Tests
Tests can be found [here](tests/). Test directory gives an outline of each test file and the purpose of each. Additionally, it contains test configurations such as a sql script that creates test tables and configuration YAML files.

## Flowchart Diagrams
Diagrams below visualize flow of execution when `DataQualityThresholdCheckOperator` and `DataQualityThresholdSQLCheckOperator` are signaled to execute.

### DataQualityThresholdCheckOperator Flowchart
![data_quality_threshold_check_operator diagram](operator_diagrams/data_quality_threshold_check_operator_flowchart.png)

### DataQualityThresholdSQLCheckOperator Flowchart
![data_quality_threshold_sql_check_operator diagram](operator_diagrams/data_quality_threshold_sql_check_operator_flowchart.png)

## Set up : Local Deploy

### Set up the Python virtual environment
`> python -m venv .`

### Set AIRFLOW_HOME
By putting the `AIRFLOW_HOME` env in the `bin/activate` file, you set the path each time you set up your venv.

`> echo "export AIRFLOW_HOME=$PWD" >> bin/activate`

### Activate your venv
`> source bin/activate`

### Install airflow
`> pip install apache-airflow`

### Initialize your Airflow DB
`> airflow initdb`

### Clone the plugin into your plugins
`> git clone https://github.com/Raybeam/rb_quality_plugin plugins/rb_quality_plugin`

### Copy over plugins requirements
`> cat plugins/rb_quality_plugin/requirements.txt >> requirements.txt`  
`> pip install -r requirements.txt`

### Set up the plugin
Move over the samples (if wanted)

`> plugins/rb_quality_plugin/bin/setup init`

`> plugins/rb_quality_plugin/bin/setup add_samples`

`> plugins/rb_quality_plugin/bin/setup add_samples --dag_only`

### Enable rbac
In the root directory of your airflow workspace, open airflow.cfg and set `rbac=True`.

### Set up a user (admin:admin)
`> airflow create_user -r Admin -u admin -e admin@example.com -f admin -l user -p admin`

### Turn on Webserver
`>airflow webserver`

### Turn on Scheduler
In a new terminal, navigate to the same directory.  
`>source bin/activate`  
`>airflow scheduler`  

### Interact with UI
In a web brower, visit localhost:8080.  

## Set up : Astronomer Deploy
### Set up local environment
Follow the local deploy [instructions](#set-up--local-deploy) for configuring your local environment.  

### Turn off Webserver and Scheduler
Either Control+C or closing the terminal's window/tab should work to turn either of them off. 

### Download Astronomer
Download astronomer package following their [tutorial](https://www.astronomer.io/docs/cli-getting-started/).

### Initialize Astronomer
In your working directory
`> astro dev init`

### Start Astronomer
`> astro dev start`
  
### Interact with UI
In a web brower, visit localhost:8080.

## Set up : Google Cloud Composer Deploy

### Clone the plugin into your plugins
`> git clone https://github.com/Raybeam/rb_quality_plugin plugins/rb_quality_plugin`

### Install gcloud 
[Install](https://cloud.google.com/sdk/docs/quickstarts) the gcloud SDK and configure it to your Cloud Composer Environment.

### Updating requirements.txt in Google Cloud Composer (CLI)
`>gcloud auth login`  

`>gcloud config set project <your Google Cloud project name>`  

`>gcloud composer environments update ENVIRONMENT_NAME --location LOCATION --update-pypi-packages-from-file=plugins/rb_quality_plugin/requirements.txt`  

`ENVIRONMENT_NAME` is the name of the environment.  
`LOCATION` is the Compute Engine region where the environment is located.  
It may take a few minutes for cloud composer to finish updating after running this command.

### Import Required Airflow Configurations
```
>gcloud composer environments update ENVIRONMENT_NAME --location LOCATION --update-airflow-configs \  
	webserver-rbac=False,\  
	core-store_serialized_dags=False,\  
	webserver-async_dagbag_loader=True,\  
	webserver-collect_dags_interval=10,\  
	webserver-dagbag_sync_interval=10,\  
	webserver-worker_refresh_interval=3600
```  

`ENVIRONMENT_NAME` is the name of the environment.  
`LOCATION` is the Compute Engine region where the environment is located.  


### Uploading Plugin to Google Cloud Composer (CLI)
Add any dags to dags folder:  
```
 >gcloud composer environments storage dags import\  
    --environment ENVIRONMENT_NAME \
    --location LOCATION \
    --source SOURCE/setup/dags/
```  

Add the plugin to plugins folder:  
```
>gcloud composer environments storage plugins import\
    --environment ENVIRONMENT_NAME \
    --location LOCATION \
    --source SOURCE
```    

`ENVIRONMENT_NAME` is the name of the environment.  
`LOCATION` is the Compute Engine region where the environment is located.  
`SOURCE` is the absolute path to the local directory (full-path/plugins/rb_quality_plugin/).  