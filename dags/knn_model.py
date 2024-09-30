from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        PythonOperator,
        PythonVirtualenvOperator,
        BranchPythonOperator
        )
import pandas as pd
import os
from sklearn.metrics import f1_score
from sklearn.neighbors import KNeighborsClassifier
import pickle
from fishmlserv.model.manager import get_model_path
with DAG(
    'knn_model',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='study',
    schedule_interval='@once',
    start_date=datetime(2024, 8, 31),
    catchup=True,
    tags=['import', 'db', 'import db'],
) as dag:
    def csvtoparquet():
        file_path="/home/manggee/data/fish_parquet/fish.parquet"
        if os.path.exists(file_path):
            return True
        else:
            os.makedirs(os.path.dirname(file_path), exist_ok = False)
        df=pd.read_csv('~/data/fish_test_data.csv')
        df.columns=['length', 'weight', 'label'] # 열 이름 변경
        def func1(x):
            if x == "Bream":
                return 0
            else:
                return 1
        df["target"]=df["label"].map(func1)
        df.to_parquet(file_path, engine = 'pyarrow', index = False)
    
    def knn_predict():
        df=pd.read_parquet('/home/manggee/data/fish_parquet/fish.parquet')
        x_test = df[['length', 'weight']]
        y_test = df['target']
        # 예측
        model_path = get_model_path()
        
        with open(model_path, 'rb') as f: # 저장
            fish_model = pickle.load(f) # 불러오기
        prediction = fish_model.predict(x_test) # 예측값
        score = f1_score(y_test, prediction) 

        # 저장
        result_df = pd.DataFrame({'target': y_test, 'pred':prediction})
        file_path="/home/manggee/data/fish_parquet/pred_result/result.parquet"
        if os.path.exists(file_path):
            return True
        else:
            os.makedirs(os.path.dirname(file_path), exist_ok = False)
        result_df.to_parquet(file_path, engine = 'pyarrow', index = False)
        print(score) # f1_score 출력
        return True



    task_start = EmptyOperator(
            task_id="start",
            )

    load_csv = PythonOperator(
            task_id="load.csv",
            python_callable=csvtoparquet
            )


    prediction = PythonOperator(
            task_id="predict",
            python_callable=knn_predict
    )
    
    aggregation = EmptyOperator(
            task_id="agg",
        )

    task_end = EmptyOperator(
            task_id="end",
    )


    task_start >> load_csv >> prediction >> aggregation >> task_end
