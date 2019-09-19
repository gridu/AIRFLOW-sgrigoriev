# GridU project for the course of Airflow

To start the airflow follow the next steps:
1. install Docker-compose if you don't have it
2. run `docker-compose up -d`
3. open `localhost:8080` in your browser
4. go to the connections and create a new connection
    - TEST_DATABASE_1
      - Connection type: Postgres
      - Host: postgres
      - Schema: airflow
      - Login: airflow
      - Password: airflow
    - (optional) TEST_DATABASE_2 (you need to create another schema for that connection)
    - fs_local
      - Connection type: File (path)

5. go to Variables and add a new one
    - wait_sensor_file_path
      - /usr/local/airflow/dags/wait_sensor2
    - monitoring_dag
      - replicate_TEST_DATABASE_1_table_records_2

Before start running DAGS, activate every dag you have (by default there are should be 4 dags).

## replicate_TEST_DATABASE_1_table_records_1
Trigger this dag and then wait until it finishes. This time we don't have a table and it should use the branch *create_table*.
Trigger this dag again and verify that it uses another branch (see it in Graph View) named *table_exists*.
(Optional) Open *query_the_table* in the Graph View - Task Instance Details - Xcom. It should have *return_value* with 2 or more records (depends on count that dag is run).

## trigger_dag
Run dag *trigger_dag* and then create a file using the followind command `touch ./dags/wait_sensor2`. Wait till it ends successfully. This trigger also should run  *replicate_TEST_DATABASE_1_table_records_2* and to wait until it finishes.