import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 5, 3)
}

###### SQL variables ######
raw_dataset = 'chicago_abandoned_2001' # dataset with raw tables
new_dataset = 'workflow' # empty dataset for destination tables
sql_cmd_start = 'bq query --use_legacy_sql=false '

sql_vehicle = 'create table ' + new_dataset + '.Abandoned_vehicle_table as ' \
	   'select distinct Vehicle_Make, Vehicle_Color, License_Plate ' \
           'from ' + raw_dataset + '.Abandoned_Data_APR_Rev ' \
	   'where License_plate is not null ' \
 	   'order by Vehicle_Make'
       
sql_request = 'create table ' + new_dataset + '.Abandoned_request_table as ' \
            'select distinct Request_Number, Service_Request_Type, Date, License_Plate, Ward ' \
            'from ' + raw_dataset + '.Abandoned_Data_APR_Rev ' \
            'where Request_Number is not null ' \
            'order by Request_Number'

sql_ward = 'create table '+ new_dataset + '.Abandoned_ward_table as ' \
            'select distinct Ward, Complete ' \
            'from ' + raw_dataset + '.Abandoned_Data_APR_Rev ' \
            'where Ward is not null ' \
            'order by Ward '

###### Beam variables ######
AIRFLOW_DAGS_DIR='/home/lc_orozco/.local/bin/dags/' # replace with your path          
LOCAL_MODE=1 # run beam jobs locally
DIST_MODE=2 # run beam jobs on Dataflow

mode=LOCAL_MODE

if mode == LOCAL_MODE:
    abandoned_script = 'transform_abandoned_data_aggregated_single.py'
    
if mode == DIST_MODE:
    abandoned_script = 'transform_abandoned_data_aggregated_cluster.py'

###### DAG section ######
with models.DAG(
        'workflow1',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    ###### SQL tasks ######
    delete_dataset = BashOperator(
            task_id='delete_dataset',
            bash_command='bq rm -r -f workflow')
    
    create_dataset = BashOperator(
	task_id='create_dataset',
	bash_command='bq mk workflow')

    create_vehicle_table = BashOperator(
	task_id='create_vehicle_table',
        bash_command=sql_cmd_start + '"' + sql_vehicle + '"')
            
    create_request_table = BashOperator(
        task_id='create_request_table',
        bash_command=sql_cmd_start + '"' + sql_request + '"')
            
    create_ward_table = BashOperator(
        task_id='create_ward_table',
        bash_command=sql_cmd_start + '"' + sql_ward + '"')

    ###### Beam tasks ######
    abandoned_beam = BashOperator(
	task_id='abandoned_beam',
	bash_command='python ' + AIRFLOW_DAGS_DIR + abandoned_script)

    transition = DummyOperator(task_id='transition')
            
    delete_dataset >> create_dataset >> [create_vehicle_table, create_request_table, create_ward_table] >> transition >> [abandoned_beam]
