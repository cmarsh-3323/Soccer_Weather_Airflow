from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
# from operators.staging_op import StageToRedshiftOperator
# from operators.load_fact_op import LoadFactOperator
# from operators.load_dims_op import LoadDimensionOperator
# from operators.data_quality_check import DataQualityOperator
# from operators.preprocess_op import PreProcessToS3Operator

from operators import StageToRedshiftOperator
from operators import LoadFactOperator
from operators import LoadDimensionOperator
from operators import DataQualityOperator
from operators import PreProcessToS3Operator
from helpers import SqlQueries

AWS_KEY = os.environ.get("AWS_KEY")
AWS_SECRET = os.environ.get("AWS_SECRET")

default_args = {
    "owner": "cmarsh",
    "start_date": datetime(2023, 10, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "email_on_retry": False,
    "depends_on_past": False,
    "catchup": False,
}

dag = DAG(
    "chris_dag",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval=None  # run once every hour
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

preprocess_data = PreProcessToS3Operator(
    task_id="Preprocess_data",
    dag=dag,
    aws_credentials_id="aws_credentials",
    aws_region="us-east-2"
)

stage_filteredweathercountrydata = StageToRedshiftOperator(
    task_id="stage_filteredweathercountrydata",
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    destination_table='public.Dim_Weather_Country',
    s3_bucket='awsbucket3323',
    s3_key='capstone/filtered_data/filtered_weather_country_data.csv',
    aws_region='us-east-2'
)

stage_league = StageToRedshiftOperator(
    task_id="stage_league",
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    destination_table='public.Dim_League',
    s3_bucket='awsbucket3323',
    s3_key='capstone/filtered_data/League.csv',
    aws_region='us-east-2'
)

stage_country = StageToRedshiftOperator(
    task_id="stage_country",
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    destination_table='public.Dim_Country',
    s3_bucket='awsbucket3323',
    s3_key='capstone/filtered_data/Country.csv',
    aws_region='us-east-2'
)

stage_team = StageToRedshiftOperator(
    task_id="stage_team",
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    destination_table='public.Dim_Team',
    s3_bucket='awsbucket3323',
    s3_key='capstone/filtered_data/Team.csv',
    aws_region='us-east-2'
)

stage_playerattributes = StageToRedshiftOperator(
    task_id="stage_playerattributes",
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    destination_table='public.Dim_Player_Attributes',
    s3_bucket='awsbucket3323',
    s3_key='capstone/filtered_data/Player_Attributes.csv',
    aws_region='us-east-2'
)

stage_match = StageToRedshiftOperator(
    task_id="stage_match",
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    destination_table='public.Fact_Match',
    s3_bucket='awsbucket3323',
    s3_key='capstone/filtered_data/Match.csv',
    aws_region='us-east-2'
)

stage_teamattributes = StageToRedshiftOperator(
    task_id="stage_teamattributes",
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    destination_table='public.Dim_Team_Attributes',
    s3_bucket='awsbucket3323',
    s3_key='capstone/filtered_data/Team_Attributes.csv',
    aws_region='us-east-2'
)

stage_player = StageToRedshiftOperator(
    task_id="stage_player",
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    destination_table='public.Dim_Player',
    s3_bucket='awsbucket3323',
    s3_key='capstone/filtered_data/Player.csv',
    aws_region='us-east-2'
)

# load_fact_match_table = LoadFactOperator(
#     task_id='Load_fact_match_table',
#     dag=dag,
#     redshift_conn_id='redshift',
#     destination_table='public.Fact_Match',
#     sql=SqlQueries.fact_match_select,
#     schema='dev',
#     sla=timedelta(hours=1)#can reduce after testing
# )

# load_weather_country_table = LoadDimensionOperator(
#     task_id='Load_weather_country_table',
#     dag=dag,
#     redshift_conn_id='redshift',
#     destination_table='public.Dim_Weather_Country',
#     sql=SqlQueries.dim_weather_country_select,
#     schema='dev',
#     sla=timedelta(minutes=30),
#     truncate=False
# )

# load_country_table = LoadDimensionOperator(
#     task_id='Load_country_table',
#     dag=dag,
#     redshift_conn_id='redshift',
#     destination_table='public.Dim_Country',
#     sql=SqlQueries.dim_country_select,
#     schema='dev',
#     sla=timedelta(minutes=30),
#     truncate=False
# )

# load_league_table = LoadDimensionOperator(
#     task_id='Load_league_table',
#     dag=dag,
#     redshift_conn_id='redshift',
#     destination_table='public.Dim_League',
#     sql=SqlQueries.dim_league_select,
#     schema='dev',
#     sla=timedelta(minutes=30),
#     truncate=False
# )

# load_team_table = LoadDimensionOperator(
#     task_id='Load_team_table',
#     dag=dag,
#     redshift_conn_id='redshift',
#     destination_table='public.Dim_Team',
#     sql=SqlQueries.dim_team_select,
#     schema='dev',
#     sla=timedelta(minutes=30),
#     truncate=False
# )

# load_team_attributes_table = LoadDimensionOperator(
#     task_id='Load_team_attributes_table',
#     dag=dag,
#     redshift_conn_id='redshift',
#     destination_table='public.Dim_Team_Attributes',
#     sql=SqlQueries.dim_team_attributes_select,
#     schema='dev',
#     sla=timedelta(minutes=30),
#     truncate=False
# )

# load_player_table = LoadDimensionOperator(
#     task_id='Load_player_table',
#     dag=dag,
#     redshift_conn_id='redshift',
#     destination_table='public.Dim_Player',
#     sql=SqlQueries.dim_player_select,
#     schema='dev',
#     sla=timedelta(minutes=30),
#     truncate=False
# )

# load_player_attributes_table = LoadDimensionOperator(
#     task_id='Load_player_attributes_table',
#     dag=dag,
#     redshift_conn_id='redshift',
#     destination_table='public.Dim_Player_Attributes',
#     sql=SqlQueries.dim_player_attributes_select,
#     schema='dev',
#     sla=timedelta(minutes=30),
#     truncate=False
# )


run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    tables=[
        "public.Fact_Match",
        "public.Dim_Weather_Country",
        "public.Dim_Country",
        "public.Dim_League",
        "public.Dim_Team",
        "public.Dim_Team_Attributes",
        "public.Dim_Player",
        "public.Dim_Player_Attributes"
    ],
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> preprocess_data
preprocess_data >> stage_filteredweathercountrydata
preprocess_data >> stage_league
preprocess_data >> stage_country
preprocess_data >> stage_team
preprocess_data >> stage_playerattributes
preprocess_data >> stage_match
preprocess_data >> stage_teamattributes
preprocess_data >> stage_player
# stage_filteredweathercountrydata >> load_weather_country_table
# stage_league >> load_league_table
# stage_country >> load_country_table
# stage_team >> load_team_table
# stage_playerattributes >> load_player_attributes_table
# stage_match >> load_fact_match_table
# stage_teamattributes >> load_team_attributes_table
# stage_player >> load_player_table
# [load_fact_match_table, load_weather_country_table, load_country_table, load_league_table, load_team_table, load_team_attributes_table, load_player_table, load_player_attributes_table] >> run_quality_checks
[stage_match, stage_filteredweathercountrydata, stage_country, stage_league, stage_team, stage_teamattributes, stage_player, stage_playerattributes] >> run_quality_checks
run_quality_checks >> end_operator

