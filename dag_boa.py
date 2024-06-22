from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
}

dag = DAG(
    'DAG_INGESTAO_DADOS_OLIST',
    default_args=default_args,
    description='Descricao do DAG',
    schedule_interval='0 */1 * * *',
)

faz_teste = BashOperator(
    task_id='coleta_dados',
    bash_command='python3 /usr/local/airflow/scripts/ingestao_dados_olist.py',
    dag=dag,
)

faz_teste
