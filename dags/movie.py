from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
with DAG(
    'movie',
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='hello world DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 26),
    catchup=True,
    tags=['movie'], 
) as dag:
    t1=DummyOperator(
        task_id='start'
        )
    t2=BashOperator(
        task_id='get.data',
        bash_command="""
        echo "get.data"
        """
        )

    t3=BashOperator(
        task_id='save.data',
        bash_command="""
        echo "save.data"
        """
        )
    t4=DummyOperator(
        task_id='end'
        )

    t1 >> t2 >> t3 >> t4
