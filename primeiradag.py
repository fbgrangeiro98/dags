from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

# Define os argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Cria uma nova DAG
dag = DAG(
    'test_dag',
    default_args=default_args,
    description='Uma DAG de teste simples',
    schedule_interval=timedelta(days=1),  # Executar diariamente
)

# Define as tarefas da DAG
start_task = DummyOperator(task_id='start_task', dag=dag)

task1 = BashOperator(
    task_id='task1',
    bash_command='echo "Executando a Task 1"',
    dag=dag,
)

task2 = BashOperator(
    task_id='task2',
    bash_command='echo "Executando a Task 2"',
    dag=dag,
)

end_task = DummyOperator(task_id='end_task', dag=dag)

# Define o fluxo de trabalho da DAG
start_task >> task1 >> task2 >> end_task
