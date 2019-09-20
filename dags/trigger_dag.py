from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.models import Variable
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator


def pull_results(dag_id, **context):
    records = context['task_instance'].xcom_pull(dag_id=dag_id,
                                                 task_ids='query_the_table',
                                                 key='return_value')
    print('%s records were written' % records)
    print(context)


def create_sub_dag(parent_dag_name, child_dag_name, start_date,
                   schedule_interval, dag_monitor, result_task, sensor_file):
    dag = DAG('%s.%s' % (parent_dag_name, child_dag_name),
              schedule_interval=schedule_interval,
              start_date=start_date)
    # monitor to the last successful end from the time this dag was run
    dag_sensor = ExternalTaskSensor(task_id='dag_monitor',
                                    external_dag_id=dag_monitor,
                                    external_task_id=None,
                                    dag=dag,
                                    check_existence=True,
                                    poke_interval=10)
    logging = PythonOperator(task_id='logging',
                             provide_context=True,
                             python_callable=pull_results,
                             op_args=[dag_monitor],
                             dag=dag)
    remove_file = BashOperator(task_id='clear_flag',
                               bash_command='rm {{ params.file_path }}',
                               params={'file_path': sensor_file},
                               dag=dag)
    finished_ts = BashOperator(task_id='log_end_time',
                               bash_command='echo {{ ts_nodash }}',
                               dag=dag)
    dag_sensor >> logging >> remove_file >> finished_ts
    return dag


start_date = datetime(2019, 1, 1)
with DAG('trigger_dag', schedule_interval=None, start_date=start_date) as dag:
    file_path = Variable.get('wait_sensor_file_path',
                             default_var='/usr/local/airflow/dags/wait_sensor')
    monitoring_dag = Variable.get(
        'monitoring_dag',
        default_var='replicate_TEST_DATABASE_1_table_records_1')
    wait_sensor = FileSensor(task_id='wait_sensor',
                             poke_interval=5,
                             fs_conn_id='fs_local',
                             filepath=file_path)
    execution_date = '{{ ts }}'
    dag_run_trigger = TriggerDagRunOperator(task_id='dag_run',
                                            trigger_dag_id=monitoring_dag,
                                            execution_date=execution_date)
    sub_dag = SubDagOperator(task_id='wait_dag',
                             subdag=create_sub_dag('trigger_dag', 'wait_dag',
                                                   start_date, None,
                                                   monitoring_dag,
                                                   'save_results', file_path))
    wait_sensor >> dag_run_trigger >> sub_dag
