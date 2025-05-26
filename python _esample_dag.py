from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime

default_args = {
  'owner':'sborisv74',
  'depends_on_past':False,
  'start_date':datetime(2025, 05, 26),
  'retries':0
}

dag = DAG('python_hello_world_dag',
      default_args = default_args,
      catchup=False,
      schedule_interval='00 20 * * *'
        )

def hello():
  return print('Hello, world!')

def sum_int():
  return print(5+8)

t1 = PythonOperator(
  task_id='print_hello_world',
  python_callable=hello,
  dag=dag
)

t2 = PythonOperator(
  task_id='print_sum',
  python_callable=sum_int,
  dag=dag
)

t1 >> t2
