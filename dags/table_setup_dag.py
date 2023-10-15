from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow import DAG
import datetime
import os

setup_dag = DAG('table_setup_dag',
          description='Drops and Creates tables',
          schedule_interval=None, #must manually trigger
          start_date=datetime.datetime(2023, 1, 1)
        )

drop_tables_task = PostgresOperator(
    task_id="drop_task",
    dag=setup_dag,
    postgres_conn_id="redshift",
    sql="drop_tables.sql"
)

create_tables_task = PostgresOperator(
    task_id="create_task",
    dag=setup_dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

drop_tables_task >> create_tables_task