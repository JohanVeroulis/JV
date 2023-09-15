from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


from datetime import datetime, timedelta
import csv
import json

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

input_csv_file = '/opt/airflow/dags/files/cvas_data_transactions.csv'
output_json_file = '/opt/airflow/dags/files/cvas_data_transactions.json'
csv_data =[]

def convert_csvfile():
    with open(input_csv_file, 'r') as data:
        reader = csv.DictReader(data, delimiter=',')
        for row in reader:
            #csv_data.append(row)
            with open(output_json_file, 'a') as outfile:
                json.dump(row, outfile, indent=4)
                #print(a)
                


with DAG(dag_id="data_pipeline", start_date=datetime(2021, 1 ,1), 
    schedule_interval="@daily", default_args=default_args, catchup=False) as dag:

    saving_data = BashOperator(
        task_id="saving_data",
        bash_command="""
            hdfs dfs -mkdir -p /data && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/cvas_data_transactions.json /data
            """
    )

    # saving_data2 = BashOperator(
    #     task_id="saving_data2",
    #     bash_command="""
    #         hdfs dfs -put -f $AIRFLOW_HOME/dags/files/subscribers.csv /data
    #         """
    # )

    # Parsing forex_pairs.csv and downloading the files
    convert_csvfile_tojson = PythonOperator(
            task_id="convert_csvfile_tojson",
            python_callable=convert_csvfile
    )
    
    # Creating a hive table named forex_rates
    creating_data_table = HiveOperator(
        task_id="creating_data_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS data_rates(
                day_id STRING,
                sub_id STRING,
                amount FLOAT,
                channel STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )
   
    #Running Spark Job to process the data
    data_processing_insert = SparkSubmitOperator(
        task_id="data_processing_insert",
        conn_id="spark_conn",
        application="/opt/airflow/dags/scripts/data_processing.py",
        verbose=False
    )

    

    


   
     

 
     




