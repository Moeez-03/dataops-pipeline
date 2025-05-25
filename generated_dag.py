from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import logging

default_args = {
    'owner': 'dataops-ai',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'customer_analytics_pipeline',
    default_args=default_args,
    description='AI-generated pipeline for customer_analytics_pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['ai-generated', 'dataops'],
)

def extract_data(**context):
    """Extract data from source"""
        ##import pandas as pd

    df = pd.read_csv("sample_customer_data.csv")
    logging.info(f"Extracted {len(df)} rows from CSV")
    return df

def transform_data(**context):
    """Transform data using AI recommendations"""
        # Start with the extracted data
    df = context["task_instance"].xcom_pull(task_ids="extract_data")
    # Remove duplicates
    df = df.drop_duplicates()
    logging.info(f"Transformed data: {{len(df)}} rows")
    return df

def validate_data(**context):
    """Validate data quality"""
        # Start with the transformed data
    df = context["task_instance"].xcom_pull(task_ids="transform_data")
    # Check for unique identifiers in customer_id
    if df['customer_id'].nunique() != len(df):
        logging.warning(f'Non-unique identifiers found in customer_id')
    # Validate email format in email
    email_valid = df['email'].str.contains('@', na=False)
    if not email_valid.all():
        logging.warning(f'Found {(~email_valid).sum()} invalid emails in email')
    logging.info('Data validation completed successfully')
    return df

def load_data(**context):
    """Load data to destination"""
        from sqlalchemy import create_engine

    df = context["task_instance"].xcom_pull(task_ids="validate_data")
    engine = create_engine('postgresql://:@localhost/analytics')
    df.to_sql('processed_customers', engine, if_exists='replace', index=False)
    logging.info(f"Loaded {len(df)} rows to PostgreSQL")

# Define tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> validate_task >> load_task
