from datetime import datetime
import uuid

from airflow import DAG, settings
from airflow.models import Connection
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.hooks.postgres_hook import PostgresHook
from operators.postgresql import PostgreSQLCountRows

config = {
        'dag_id_1': {'schedule_interval': None, 'start_date': datetime(2018, 11, 11), 'table_name': 'records_1'},  
        'dag_id_2': {'schedule_interval': None, 'start_date': datetime(2018, 11, 11), 'table_name': 'records_2'},  
        'dag_id_3': {'schedule_interval': None, 'start_date': datetime(2018, 11, 11), 'table_name': 'records_3'}
}

def create_new_dags(dag_id, database): 

  def f_print_log():
    return '{} start processing tables in database: {}'.format(dag_id, database)

  def f_check_table_exists(table_name):
    connect = PostgresHook(postgres_conn_id=database)
    query = """
            select count(1) 
              from information_schema.tables 
             where table_schema not like  %s
               and table_name = %s
            """
    res = connect.get_first(query, parameters=('', table_name))
    if res[0] == 0:
      return 'create_table'
    else:
      return 'table_exists'
  
  def f_create_table(table_name):
    connect = PostgresHook(postgres_conn_id=database)
    query = """
            create table {}( 
              id integer not null,
              "user" varchar(50) not null,
              timestamp timestamp not null
             )""".format(table_name)
    connect.run(query)
  
  def f_insert_row(table_name, **context):
    connect = PostgresHook(postgres_conn_id=database)
    user = context['ti'].xcom_pull(task_ids='get_current_user', key='return_value')
    # do not do this in production, sql injection is possible
    query = """
            insert into {}
            values(%s, %s, %s)
            """.format(table_name)
    connect.run(query, parameters=(uuid.uuid4().int % 123456789, user, datetime.now()))

  # for every table replicate records
  for key in config: 
    table_name = config[key].get('table_name')
    name = '{}_table_{}'.format(dag_id, table_name)
    with DAG(name, schedule_interval = config[key].get('schedule_interval'), start_date = config[key].get('start_date')) as dag:
      # ignore the previous tasks, no backfilling
      latest_only = LatestOnlyOperator(task_id = 'latest_only', dag = dag)
      # logging the dag
      print_the_context = PythonOperator(
                  task_id = 'print_the_context', 
                  python_callable = f_print_log, 
                  dag = dag)
      # write the current username to xcom
      get_current_user = BashOperator(
               task_id = 'get_current_user',
               bash_command = 'whoami',
               xcom_push = True,
               dag = dag)
      # check table exists
      check_table_exists = BranchPythonOperator(
          task_id = 'check_table_exists',
          python_callable=f_check_table_exists,
          op_args=[table_name],
          dag = dag)
      
      # create table
      create_table = PythonOperator(
              task_id = 'create_table',
              python_callable = f_create_table,
              op_args=[table_name],
              dag = dag)
      # skip table generation
      table_exists = DummyOperator(task_id = 'table_exists', dag = dag)

      # insert to a table
      insert_new_rows = PythonOperator(
              task_id = 'insert_new_rows',
              python_callable = f_insert_row, 
              op_kwargs = {'table_name': table_name},
              provide_context = True,
              dag = dag, 
              trigger_rule='none_failed')
      # query a table
      query_the_table = PostgreSQLCountRows(
              task_id = 'query_the_table',
              table_name = table_name,
              connection_id = database,
              dag = dag)
      
      latest_only >> print_the_context >> get_current_user >> check_table_exists >> (create_table, table_exists) >> insert_new_rows >> query_the_table

      yield name,dag

session = settings.Session()
conns = (session.query(Connection.conn_id)
                .filter(Connection.conn_id.ilike('TEST_DATABASE%'))
                .all())

for conn in conns:
  dag_name = 'replicate_{}'.format(conn[0])
  dags = create_new_dags(dag_name, conn[0])
  for (name, dag) in dags:
    globals()[name] = dag
