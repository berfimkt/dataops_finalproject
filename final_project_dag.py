from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.models import Variable
from datetime import datetime, timedelta
import os, sys
from airflow import DAG

start_date = datetime(2023, 10, 11)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

ssh_hook = SSHHook(ssh_conn_id="spark_ssh_conn", cmd_timeout=None)

with DAG('final_project', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    
    t1 = SSHOperator(task_id='credits_datagen_to_s3',
                    command=f"""cd /home/ssh_train/ && 
                    source datagen/bin/activate && 
                    python /home/ssh_train/data-generator/dataframe_to_s3.py -buc tmdb-bronze -k credits/credits_part -aki dataopsadmin -sac dataopsadmin -eu http://minio:9000 -i /home/ssh_train/datasets/tmdb_5000_movies_and_credits/tmdb_5000_credits.csv -ofp True -z 500 -b 0.1""",
                    execution_timeout=None,
                    ssh_conn_id='spark_ssh_conn',
                    ssh_hook=ssh_hook)
    
    t2 = SSHOperator(task_id='movies_datagen_to_s3',
                    command=f"""cd /home/ssh_train/ && 
                    source datagen/bin/activate && 
                    python /home/ssh_train/data-generator/dataframe_to_s3.py -buc tmdb-bronze -k   movies/movies_part -aki dataopsadmin -sac dataopsadmin -eu http://minio:9000 -i /home/ssh_train/datasets/tmdb_5000_movies_and_credits/tmdb_5000_movies.csv -ofp True -z 500 -b 0.1""",
                    execution_timeout=None,
                    ssh_conn_id='spark_ssh_conn',
                    ssh_hook=ssh_hook)
    
    t3 = DummyOperator(task_id='generated_data')
  

    t4 = SSHOperator(task_id='s3_to_Spark_to_s3',
                    command=f"""source /dataops/schedulenv/bin/activate &&
                    export PATH=/opt/spark/bin:$PATH &&
                    spark-submit /dataops/final_project.py""",
                    ssh_conn_id='spark_ssh_conn',
                    ssh_hook=ssh_hook)

    [t1,t2] >> t3 >> t4