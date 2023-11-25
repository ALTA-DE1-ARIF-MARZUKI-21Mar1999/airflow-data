from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

with DAG(
    'alterra_day1_task',
    description='Task day 1',
    schedule_interval='* */5 * * *',
    start_date=datetime(2022, 10, 21),
    catchup=False
) as dag:
    @task
    def push_multiple_values_to_xcom(ti=None):
        config_data = Variable.get('config_data', deserialize_json=True)
        fruit_names = Variable.get('fruit_names', deserialize_json=True)
        
        multiple_values = [config_data, fruit_names]
        ti.xcom_push(key='multiple_values', value=multiple_values)

    @task
    def get_multiple_values_from_xcom(ti=None):
        multiple_value = ti.xcom_pull(task_ids='push_multiple_values_to_xcom', key='multiple_values')
        print(f'print multiple_values variable from xcom: {multiple_value}')

    push_multiple_values_to_xcom() >> get_multiple_values_from_xcom()
