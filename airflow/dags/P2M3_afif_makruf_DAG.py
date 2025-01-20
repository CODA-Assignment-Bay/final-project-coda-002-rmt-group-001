import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

path = '/opt/airflow/dags/'
# Konfigurasi default dari argumen
default_args = {
    'owner': 'afif',
    'start_date': dt.datetime(2025, 1, 12), # Atur jadwal dimulainya program
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=600),
}


with DAG('ETL_influencer_merchandise',
         description='Proses ETL pipeline untuk data influencer merchandise',
         schedule_interval='10-30/10 9 * * 6', # Atur interval penjalanan program --> Diatur tiap hari sabtu jam 9.10 - 9.30 dengan interval 10 menit
         default_args=default_args,
         ) as dag:

    # Atur penjadwalan urutan kode yang dijalankan
    extract = BashOperator(task_id='data_extract',
                               bash_command=f'sudo -u airflow python {path}extract.py')
    transform = BashOperator(task_id='data_transform',
                               bash_command=f'sudo -u airflow python {path}transform.py')
    load = BashOperator(task_id='data_load',
                               bash_command=f'sudo -u airflow python {path}load.py')
    
# Urutan eksekusi program
extract >> transform >> load