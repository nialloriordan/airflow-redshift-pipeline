from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers.sql_queries import SqlQueries

default_args = {
    "owner": "niall_o_riordan",
    "start_date": datetime(21, 7, 17),
    "depends_on_past": False,
    "retries": 2,
    "retries_delay": timedelta(minutes=5),
    "email": ["oriordanniall1@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "catchup": False,
}

with DAG(
    "etl_s3_redshift",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
    max_active_runs=1,
) as dag:

    start_operator = DummyOperator(task_id="Begin_execution")

    create_tables = PostgresOperator(
        task_id="create_required_tables",
        postgres_conn_id="redshift",
        sql=SqlQueries.create_tables,
    )

    with TaskGroup(group_id="stage_events") as group_stage_events:

        stage_events_to_redshift = StageToRedshiftOperator(
            task_id="Stage_events",
            redshift_conn_id="redshift",
            aws_credentials_id="aws_credentials",
            table="staging_events",
            s3_bucket="udacity-dend",
            s3_key="log_data",
            s3_region="us-west-2",
            json_schema="s3://udacity-dend/log_json_path.json",
            truncate=False,
        )

        stage_songs_to_redshift = StageToRedshiftOperator(
            task_id="Stage_songs",
            redshift_conn_id="redshift",
            aws_credentials_id="aws_credentials",
            table="staging_songs",
            s3_bucket="udacity-dend",
            s3_key="song_data",
            s3_region="us-west-2",
            truncate=False,
        )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift_conn_id="redshift",
        sql_statement=SqlQueries.songplay_table_insert,
        target_table="songplays",
    )

    with TaskGroup(group_id="load_dimension_tables") as group_load_dim_tables:

        load_user_dimension_table = LoadDimensionOperator(
            redshift_conn_id="redshift",
            task_id="Load_user_dim_table",
            sql_statement=SqlQueries.user_table_insert,
            target_table="users",
            truncate=False,
        )

        load_song_dimension_table = LoadDimensionOperator(
            redshift_conn_id="redshift",
            task_id="Load_song_dim_table",
            sql_statement=SqlQueries.song_table_insert,
            target_table="songs",
            truncate=False,
        )

        load_artist_dimension_table = LoadDimensionOperator(
            redshift_conn_id="redshift",
            task_id="Load_artist_dim_table",
            sql_statement=SqlQueries.artist_table_insert,
            target_table="artists",
            truncate=False,
        )

        load_time_dimension_table = LoadDimensionOperator(
            redshift_conn_id="redshift",
            task_id="Load_time_dim_table",
            sql_statement=SqlQueries.time_table_insert,
            target_table="time",
            truncate=False,
        )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        redshift_conn_id="redshift",
        sql_queries_results={
            "SELECT COUNT(*) FROM songs WHERE songid IS NULL": lambda num_records: num_records == 0,
            "SELECT COUNT(*) FROM staging_events": lambda num_records: num_records > 0,
            "SELECT COUNT(*) FROM staging_songs": lambda num_records: num_records > 0,
            "SELECT COUNT(*) FROM users WHERE userid IS NULL": lambda num_records: num_records == 0,
            "SELECT COUNT(*) FROM artists WHERE artistid IS NULL": lambda num_records: num_records
            == 0,
        },
    )

    end_operator = DummyOperator(task_id="Stop_execution")

    (
        start_operator
        >> create_tables
        >> group_stage_events
        >> load_songplays_table
        >> group_load_dim_tables
        >> run_quality_checks
        >> end_operator
    )
