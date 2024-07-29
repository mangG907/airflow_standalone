from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator 
from airflow.operators.python import PythonOperator
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
        key = get_key()
        print(f"MOVIE_API_KEY => {KEY}")
        df = save2df()
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

    run_this = PythonOperator(
            task_id="print_the_context",
            python_callable=print_context)

    t1=EmptyOperator(
        task_id='start'
        )

    t2=PythonOperator(
        task_id='get.data',
        python_callable=get_data
        )

    t3=BashOperator(
        task_id='save.data',
        bash_command="""
        echo "save.data"
        """
        )

    t4=EmptyOperator(
        task_id='end'
        )

    t1 >> t2 >> t3 >> t4
    t1 >> run_this >> t4
