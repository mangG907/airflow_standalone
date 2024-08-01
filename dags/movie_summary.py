from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator,
    PythonVirtualenvOperator,
    is_venv_installed,
)
from airflow.models import Variable
from pprint import pprint

with DAG(
    'movie_summary',
    default_args={
        'depends_on_past': False,
        #'email_on_failure': False,
        #'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    #max_active_tasks=3,
    #max_active_runs=1,
    description='movie summary',
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 20),
    catchup=True,
    tags=['movie', 'summary', 'etl', 'shop'],
) as dag:

    apply_type = EmptyOperator(
        task_id="apply.type",
        )
    
    merge_df = EmptyOperator(
        task_id="merge.of",
        )

    de_dup = EmptyOperator(
        task_id="de.dup",
        )

    summary_df = EmptyOperator(
        task_id="summary.df",
        )

    task_end = EmptyOperator(task_id='end', trigger_rule='all_done')

    task_start = EmptyOperator(task_id='start')


    task_start >> apply_type >> merge_df >> de_dup >> summary_df >> task_end
