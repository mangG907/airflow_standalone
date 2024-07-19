from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
with DAG(
        'import_db',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='hello world DAG',
    #schedule=timedelta(days=1),
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['simple', 'bash', 'etl', 'shop'],
) as dag:

    task_check = BashOperator(
        task_id="check",
        bash_command="bash {{ var.value.CHECK_SH }} {{ds_nodash}}"
        
    )

    task_csv = BashOperator(
        task_id="to.csv",
        bash_command="""
            echo "to.csv"

            U_PATH=~/data/count/{{ds_nodash}}/count.log
            CSV_PATH=~/data/csv/{{ds_nodash}}

            mkdir -p $CSV_PATH


            cat ${U_PATH} | awk '{print "{{ds}}," $2 "," $1}' > ${CSV_PATH}/csv.csv
        """,
            trigger_rule="all_success"
    )

    task_tmp = BashOperator(
            task_id="to.tmp",
            bash_command="""
                echo "sort"
                # Generate Directory
                 mkdir -p ~/data/sort/{{ds_nodash}}

                # Store sort result file
        cat ~/data/cut/{{ds_nodash}}/cut.log | sort > ~/data/sort/{{ds_nodash}}/sort.log
            """
    )

    task_base = BashOperator(
            task_id="to.base",
            bash_command="""
                echo "count"
                # Repeat
            mkdir -p ~/data/count/{{ds_nodash}}
                cat ~/data/sort/{{ds_nodash}}/sort.log | uniq -c > ~/data/count/{{ds_nodash}}/count.log
               """
    )

    task_err = BashOperator(
                task_id="err.report",
                bash_command="""
                echo "err report"
            """,
            trigger_rule="one_failed"
    )

    task_done = BashOperator(
                task_id="make.done",
                bash_command="""
            
            """,
            trigger_rule="one_failed"
    )
     
    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_start = EmptyOperator(task_id='start')

    task_start >> task_check >> task_csv >> task_tmp >> task_base >> task_done >> task_end
    task_check >> task_err >> task_end
 
