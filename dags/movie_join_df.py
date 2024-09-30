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
    'movie',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_tasks=3,
    max_active_runs=1,
    description='hello world DAG',
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 25),
    catchup=True,
    tags=['api', 'movie'], 
) as dag:


    run_this = PythonOperator(
            task_id="print_the_context",
            python_callable=print_context,
            )


    task_get=PythonVirtualenvOperator(
        task_id='get.data',
        python_callable=get_data,
        requirements=["git+https://github.com/mangG907/movie.git@0.2.0/mov"],
        system_site_packages=False,
        trigger_rule="all_done",
        #venv_cache_path="/home/kim1/tmp2/airflow_venv/get_data"
        )


    save_data=PythonVirtualenvOperator(
        task_id='save.data',
        python_callable=save_data,
        system_site_packages=False,
        trigger_rule="one_success",
        requirements=["git+https://github.com/mangG907/movie.git@0.2.0/mov"],
        )



    rm_dir=BashOperator(
        task_id='rm.dir',
        bash_command="""
        rm -rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}""",
        trigger_rule="all_done"
        )


    echo_task = BashOperator(
        task_id='echo.task',
        bash_command="echo 'task'",
        trigger_rule="all_success"
        )



    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_start = EmptyOperator(task_id='start')

