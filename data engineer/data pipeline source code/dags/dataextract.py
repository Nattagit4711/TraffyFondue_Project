import traffy_extract
import PysparkEditCSV
import upload_data
#datetime
from datetime import timedelta, datetime

# The DAG object
from airflow import DAG

# Operators
from airflow.operators.python import PythonOperator

#initializing the default arguments
default_args = {
    "owner": "Rawit",
    "start_date" : datetime(2022, 12, 8),    
}

#Instantiate a DAG object 
data_pipeline = DAG("data_pipeline",
                default_args=default_args,
                description="data_pipeline DAG",
                schedule="0 0 * * 0",
                catchup=False,
                tags=['data_preprocessing', 'data_extraction'])

#python callable function
data_extraction = PythonOperator(task_id="dataextraction", python_callable=traffy_extract.extract_data, dag=data_pipeline)
data_cleaning = PythonOperator(task_id='datacleaning', python_callable=PysparkEditCSV.csvedit, dag=data_pipeline)
data_preparation = PythonOperator(task_id="datapreparation", python_callable=upload_data.upload, dag=data_pipeline)
#set the order of execution of task.
data_extraction >> data_cleaning >> data_preparation

