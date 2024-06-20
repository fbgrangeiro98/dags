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
    'DAG_INGESTAO_DADOS_EXAMES',
    default_args=default_args,
    description='Atividade DAG ingestao Exames',
    schedule_interval='0 */2 * * *',
)

carregar_dados_camada_raw = BashOperator(
    task_id='carregar_dados_camada_raw',
    bash_command='python3 /usr/local/airflow/scripts/CARREGAR_DADOS_CAMADA_RAW.py',
    dag=dag,
)

normalizar_dados = BashOperator(
    task_id='normalizar_dados',
    bash_command='python3 /usr/local/airflow/scripts/NORMALIZA_DADOS.py',
    dag=dag,
)

carregar_dados_consume_zone = BashOperator(
    task_id='carregar_dados_consume_zone',
    bash_command='python3 /usr/local/airflow/scripts/CARREGAR_DADOS_CONSUME_ZONE.py',
    dag=dag,
)

carregar_dados_camada_raw >> normalizar_dados >> carregar_dados_consume_zone