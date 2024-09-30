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
from pprint import pprint as pp

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
    REQUIREMENTS = [
            "git+https://github.com/mangG907/mov_agg.git@0.5.0/agg",
                ]

    def gen_empty(*ids):
        tasks = []
        for id in ids:
            task = EmptyOperator(task_id=id)
            tasks.append(task)
        return tuple(tasks)

    def gen_vpython(**kw):
        id = kw['id']
        id = id
        fun_o = kw['fun_obj']
        op_kw = kw['op_kw']

        task = PythonVirtualenvOperator(
                task_id=id,
                python_callable=kw['fun_obj'],
                system_site_packages=False,
                requirements=REQUIREMENTS,
                op_kwargs=kw['op_kw']
            )
        return task

    def pro_data(**params):
        print("@" * 33)
        print(params['task_name'])
        from pprint import pprint as pp
        pp(params) # 여기는 task_name
        print("@" * 33)

    def pro_merge(task_name, **params):
        load_dt = params['ds_nodash']
        from mov_agg.u import merge
        df = merge(load_dt)
        print("*" * 33)
        print(df)

    def pro_data3(task_name):
        print("@" * 33)
        print(task_name)
        #print(params) # 여기는 task_name 없을 것으로 예상 
        print("@" * 33)

    def pro_data4(task_name, ds_nodash, **kwargs):
        print("@" * 33)
        print(task_name)
        print(ds_nodash) # 여기는 task_name, ds_nodash가 없을 것으로 예상됨.
        print(kwargs)
        print("@" * 33)

    start, end = gen_empty('start', 'end')

    apply_type = gen_vpython(
            id = "apply.type",
            fun_obj = pro_data,
            op_kw = {"task_name": "apply_type!!"}
            )

    merge_df = gen_vpython(
            id= "merge.df",
            fun_obj = pro_merge,
            op_kw = {"task_name" : "merge_df!!"} 
            )

    de_dup = gen_vpython(
            id= "de.dup",
            fun_obj = pro_data3,
            op_kw = {"task_name" : "de_dup!!"}
            )

    summary_df = gen_vpython(
            id= "summary.df",
            fun_obj = pro_data4,
            op_kw = {"task_name" : "summary_df!!"}
            )


    start >> merge_df >> de_dup >> apply_type >> summary_df >> end
