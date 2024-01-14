import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


DAG_START_DATE = datetime(2023, 1, 14, 15, 50)

DAG_DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': DAG_START_DATE,
    'retries': 2,
    'retry_delay': timedelta(seconds = 5)
}


with DAG('bash_csv_dag', default_args= DAG_DEFAULT_ARGS, schedule_interval='0 1 * * *') as dag:

    task_1 = BashOperator(
        task_id ="bash_example",
        bash_command = "cat /home/giambar/Git/docker-airflow/dags/movie_short.csv ",
        dag=dag,
)
    task_1