"""
tvkDAGv2.py  DAG to impliment the Automate Data Pipelines final project
used for: Udacity Automate Data Piplines Project
See README.md file for complete list of sources and implementation details
details
2023-08-02
version 2.1 
    correct SQL errors
"""
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from final_project_operators.sqlqueries_tvk import SqlQueries

from final_project_operators.stage_redshift_tvk import StageToRedshiftOperator
from final_project_operators.data_quality_tvk import DataQualityOperator
from final_project_operators.load_dimension_tvk import LoadDimensionOperator
from final_project_operators.load_fact_tvk import LoadFactOperator

# Default args per project specifications
default_args = {
    'owner': 'T_van_Kessel',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup_by_default': False,
    'email_on_retry': False
}
# define dag defaults
dag = DAG('tvkDAGv2',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup = False
        )
# define start operator task
start_operator = DummyOperator(
    task_id='Begin_execution',  
    dag=dag
    )

# define stage events task
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table = "staging_events",
    s3_path = "s3://tgvkbucket/log-data",
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    region="us-east-1",
    data_format="JSON",
    sql = "staging_events_table_create"
)
# define stage songs task
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table = "staging_songs",
    s3_path = "s3://tgvkbucket/song-data/A/A/",
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    region="us-east-1",
    data_format="JSON",
    sql = "staging_songs_table_create"
)
# define fact load task
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplay",
    sql_create = "songplay_table_create",
    sql="songplay_table_insert",
    append_only=False
)
# define dimension load task
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql_create = "user_table_create",
    sql="user_table_insert",
    append_only=False
)
# define dimension load task
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="song",
    sql_create = "song_table_create",
    sql="song_table_insert",
    append_only=False
)
# define dimension load task
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artist",
    sql_create = "artist_table_create",
    sql="artist_table_insert",
    append_only=False
)
# define dimension load task
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql_create = "time_table_create",
    sql="time_table_insert",
    append_only=False
)
# define quality check task
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["songplay", "users", "song", "artist", "time"]
)
# define end task
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# DAG task sequence structure

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
