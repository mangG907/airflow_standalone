from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
with DAG(
    'make_parquet',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='parquet db',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['parquet', 'make parquet'],
) as dag:




    task_check = BashOperator(
            task_id="check.done",
            bash_command="""
                DONE_FILE={{ var.value.IMPORT_DONE_PATH }}/{{ds_nodash}}/_DONE
                #DONE_FILE=/home/manggee/done/import/20240715/_DONE
                bash {{ var.value.CHECK_SH }} $DONE_FILE
            """


    )

    task_parquet = BashOperator(
            task_id="to.parquet",
            bash_command="""
            echo "to.parquet"

            READ_PATH="~/data/csv/{{ds_nodash}}/csv.csv"
            SAVE_PATH="~/data/parquet"

            python ~/airflow/py/csv2parquet.py $READ_PATH $SAVE_PATH
            """
    )

    task_done = BashOperator(
            task_id="make.done",
            bash_command="""
                figlet "make.done.start"
                DONE_PATH={{ var.value.IMPORT_DONE_PATH }}/{{ds_nodash}}
                mkdir -p $DONE_PATH
                echo "IMPORT_DONE_PATH=$DONE_PATH"
                touch $DONE_PATH/_DONE
                figlet "make.done.end"
            """
    )

    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")

    task_start >> task_check >> task_parquet >> task_done
    task_done >> task_end
