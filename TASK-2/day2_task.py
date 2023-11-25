from airflow import DAG
from datetime import datetime
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.postgres_operator import PostgresOperator
import json
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.python import PythonOperator
import pendulum

with DAG(
    dag_id = 'alterra_day2_task',
    schedule=None,
    start_date=datetime(2022, 10, 21),
    catchup=False
) as dag:
    
    identify_name = SimpleHttpOperator(
        task_id="post_name",
        endpoint="/gender/by-first-name-multiple",
        method="POST",
        data='[{"first_name":"Sandra","country":"US"},{"first_name":"Jason","country":"US"}]',
        http_conn_id="gender_api",
        log_response=True,
        dag=dag
    )

    def my_uri():
        from airflow.hooks.base import BaseHook
        print(f"Gender API URI ", BaseHook.get_connection("gender_api").get_uri())

    print_uri = PythonOperator(
        task_id = "print_uri",
        python_callable = my_uri
    )
    
    create_table_in_db_task = PostgresOperator(
        task_id = 'create_table_in_db',
        sql = ('CREATE TABLE IF NOT EXISTS gender_api ' +
        '(' +
            'input TEXT, ' +
            'details TEXT, ' +
            'result_found VARCHAR(50), ' +
            'first_name VARCHAR(50), ' +
            'probability FLOAT, ' +
            'gender VARCHAR(50), ' +
            'timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP' +
        ')'),
        postgres_conn_id='pg_conn_id', 
        autocommit=True,
        dag=dag
    )

    def loadDataToPostgres(**context):
        ti = context['ti']
        result = ti.xcom_pull(task_ids='post_name')
        json_data = json.loads(result)

        pg_hook = PostgresHook(postgres_conn_id='pg_conn_id').get_conn()
        with pg_hook.cursor() as cursor:
            for data in json_data:
                first_name = data.get('first_name')
                input_data = json.dumps(data.get('input'))
                result_found = data.get('result_found')
                probability = data.get('probability')
                gender = data.get('gender')
                details = json.dumps(data.get('details'))

                cursor.execute(
                    f"INSERT INTO gender_api (first_name, input, result_found, probability, gender, details) "
                    f"VALUES ('{first_name}', '{input_data}', '{result_found}', '{probability}', '{gender}', '{details}')"
                )
        pg_hook.commit()
        
    load_data_to_db_task = PythonOperator(
        task_id='load_data_to_db',
        python_callable=loadDataToPostgres,
        provide_context=True,
        dag=dag
    )

identify_name >> print_uri >> create_table_in_db_task >> load_data_to_db_task