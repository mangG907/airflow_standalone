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

def print_context(ds=None, **kwargs):
    """Print the Airflow context and ds variable from the context."""
    print("::group::All kwargs")
    pprint(kwargs)
    print("::endgroup::")
    print("::group::Context variable ds")
    print(ds)
    print("::endgroup::")
    return "Whatever you return gets printed in the logs"

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

    def save_data(ds_nodash):
        from mov.api.call import apply_type2df
        df=apply_type2df(load_dt=ds_nodash)

    def get_data(ds_nodash):
        from mov.api.call import list2df, get_key, save2df
        df = save2df(ds_nodash)
        print(df.head(5))
    
    def fun_multi_y(ds_nodash, arg):
        from mov.api.call import save2df
        #p= {"multiMovieYn" : "Y"}
        df = save2df(load_dt=ds_nodash, url_param=arg)
        print(df.head(5))
    
    def branch_fun(**kwargs):
        ld = kwargs['ds_nodash']
        import os
        home_dir = os.path.expanduser("~")
        path = f'{home_dir}/tmp/test_parquet/load_dt={ld}'
        if os.path.exists(path):
            return "rm.dir"
        else:
            return "get.data", "echo.task"


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

    #다양성 영화 유무
    multi_y = PythonVirtualenvOperator(
        task_id='multi.y',
        python_callable=fun_multi_y,
        system_site_packages=False,
        requirements=["git+https://github.com/mangG907/movie.git@0.2.0/mov"],
        op_kwargs={ "arg" : {"multiMovieYn" : "Y"}}
        )


    multi_n = PythonVirtualenvOperator(
        task_id='multi.n',
        python_callable=fun_multi_y,
        system_site_packages=False,
        requirements=["git+https://github.com/mangG907/movie.git@0.2.0/mov"],
        op_kwargs={ "arg" : {"multiMovieYn" : "N"}}
        )

    nation_k = PythonVirtualenvOperator(
        task_id='nation.k',
        python_callable=fun_multi_y,
        system_site_packages=False,
        requirements=["git+https://github.com/mangG907/movie.git@0.2.0/mov"],
        op_kwargs={ "arg" : {"repNationCd" : "K"}}
        )

    nation_f = PythonVirtualenvOperator(
        task_id='nation.f',
        python_callable=fun_multi_y,
        system_site_packages=False,
        requirements=["git+https://github.com/mangG907/movie.git@0.2.0/mov"],
        op_kwargs={ "arg" : {"repNationCd" : "F"}}
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
    get_start = EmptyOperator(task_id="get.start",trigger_rule="all_done")
    get_end = EmptyOperator(task_id="get.end",trigger_rule="all_done" )
    throw_err = BashOperator(
        task_id='throw.err',
        bash_command="exit 1",
        trigger_rule="all_done"
        )

    task_start >> branch_op
    branch_op >> rm_dir >> get_start
    branch_op >> echo_task >> get_start
    branch_op >> get_start
    task_start >> throw_err >> get_start

    get_start >> [task_get, multi_y, multi_n, nation_k, nation_f] >> get_end
    get_end >> save_data >> task_end


