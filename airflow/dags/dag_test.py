from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'dag_test',
    default_args=default_args,
    description='DAG simples de teste',
    schedule_interval='@daily',
    catchup=False,
)

def task_1():
    print("Executando tarefa 1")

def task_2():
    print("Executando tarefa 2")

def task_3():
    print("Executando tarefa 3")

t1 = PythonOperator(
    task_id='task_1',
    python_callable=task_1,
    dag=dag,
)

t2 = PythonOperator(
    task_id='task_2',
    python_callable=task_2,
    dag=dag,
)

t3 = PythonOperator(
    task_id='task_3',
    python_callable=task_3,
    dag=dag,
)

t1 >> t2 >> t3