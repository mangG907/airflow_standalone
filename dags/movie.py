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
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='movie',
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['movie'], 
) as dag:
    def get_data(ds, **kwargs):
        print(ds)
        print(kwargs)
        print("=" * 20)
        print(f"ds_nodash => {kwargs['ds_nodash']}")
        print(f"kwargs type => {type(kwargs)}")
        print("=" * 20)
        from mov.api.call import get_key, save2df
        KEY = get_key()
        print(f"MOVIE_API_KEY => {KEY}")
        YYYYMMDD = kwargs['ds_nodash']
        df = save2df(YYYYMMDD)
        print(df.head(5))

    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        print("::group::All kwargs")
        pprint(kwargs)
        print("::endgroup::")
        print("::group::Context variable ds")
        print(ds)
        print("::endgroup::")
        return "Whatever you return gets printed in the logs"


    def branch_fun(**kwargs):
        ds_nodash = kwargs['ds_nodash']
        import os
        if os.path.exists(f'~/tmp/test_parquet/load_dt={ld}'):
            return rm_dir
        else:
            return get_data


    branch_op = BranchPythonOperator(
        task_id="branch.op",
        python_callable=branch_fun
    )


    run_this = PythonOperator(
            task_id="print_the_context",
            python_callable=print_context,
            )

    start=EmptyOperator(
        task_id='start'
        )

    get_data=PythonVirtualenvOperator(
        task_id='get.data',
        python_callable=get_data,
        requirements=["git+https://github.com/mangG907/movie.git@0.2.0/mov"],
        system_site_packages=False,
        )

    save_data=BashOperator(
        task_id='save.data',
        bash_command="""
        echo "save.data"
        """
        )
    rm_dir=BashOperator(
            task_id='rm.dir',
            bash_command='rm-rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}',
        )

    end=EmptyOperator(
        task_id='end'
        )

    start >> branch_op
    branch_op >> rm_dir
    branch_op >> get_data

    get_data >> save_data >> end
    rm_dir >> get_data
