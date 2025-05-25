from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import pandas as pd
import logging

# DAG Configuration
default_args = {
    'owner': 'dataops-ai',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'customer-analyte',
    default_args=default_args,
    description='AI-generated data pipeline for customer-analyte',
    schedule_interval='@daily',
    catchup=False,
    tags=['ai-generated', 'dataops']
)

def extract_data(**context):
    """Extract data from source"""
    logging.info("Starting data extraction...")
    # Add your data extraction logic here
    df = pd.read_csv('input_data.csv')
    logging.info(f"Extracted {len(df)} rows")
    return df.to_json()

def transform_data(**context):
    """Apply AI-suggested transformations"""
    logging.info("Starting data transformation...")
    ti = context['ti']
    json_data = ti.xcom_pull(task_ids='extract_data')
    df = pd.read_json(json_data)
    
    # AI-generated transformations
        # Remove duplicate rows and handle missing values
    # df.drop_duplicates().fillna(method="forward")
    # Normalize Index values
    # df['Index_normalized'] = (df['Index'] - df['Index'].mean()) / df['Index'].std()
    # Normalize Phone 1 values
    # df['Phone 1_normalized'] = (df['Phone 1'] - df['Phone 1'].mean()) / df['Phone 1'].std()
    # Normalize Phone 2 values
    # df['Phone 2_normalized'] = (df['Phone 2'] - df['Phone 2'].mean()) / df['Phone 2'].std()
    
    logging.info(f"Transformed data: {len(df)} rows, {len(df.columns)} columns")
    return df.to_json()

def quality_checks(**context):
    """Run AI-powered quality checks"""
    logging.info("Running quality checks...")
    ti = context['ti']
    json_data = ti.xcom_pull(task_ids='transform_data')
    df = pd.read_json(json_data)
    
    # Quality checks
        # Check for missing values across all columns (high priority)
    # Validate unique constraints where applicable (medium priority)
    # Check data format consistency (medium priority)
    
    logging.info("Quality checks completed successfully")
    return json_data

def load_data(**context):
    """Load data to destination"""
    logging.info("Loading data to csv...")
    ti = context['ti']
    json_data = ti.xcom_pull(task_ids='quality_checks')
    df = pd.read_json(json_data)
    
    # Load to destination
    df.to_csv('output.csv', index=False)
    
    logging.info(f"Successfully loaded {len(df)} rows to csv")

# Define tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

quality_task = PythonOperator(
    task_id='quality_checks',
    python_callable=quality_checks,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

# Set task dependencies
extract_task >> transform_task >> quality_task >> load_task