#!/usr/bin/env python3
"""
DataOps: AI-Powered Automated Data Pipeline Builder
A comprehensive Python framework for intelligent data pipeline creation and management
"""

import pandas as pd
import numpy as np
import json
import yaml
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from pathlib import Path
import pickle
import re
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.linear_model import LinearRegression
import warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class PipelineConfig:
    """Configuration for data pipeline"""
    name: str
    source_type: str
    source_config: Dict[str, Any]
    transformations: List[Dict[str, Any]]
    destination_type: str
    destination_config: Dict[str, Any]
    schedule: str
    quality_checks: List[Dict[str, Any]]
    framework: str = "airflow"
    created_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()

class AIDataAnalyzer:
    """AI-powered data analysis and pattern recognition"""
    
    def __init__(self):
        self.scaler = StandardScaler()
        self.anomaly_detector = IsolationForest(contamination=0.1, random_state=42)
        self.pattern_model = KMeans(n_clusters=5, random_state=42)
        
    def analyze_data_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze data patterns using ML algorithms"""
        analysis_results = {
            'data_quality': self._assess_data_quality(df),
            'column_types': self._infer_column_types(df),
            'patterns': self._detect_patterns(df),
            'anomalies': self._detect_anomalies(df),
            'correlations': self._analyze_correlations(df),
            'recommendations': []
        }
        
        # Generate AI recommendations
        analysis_results['recommendations'] = self._generate_recommendations(analysis_results, df)
        
        return analysis_results
    
    def _assess_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Assess data quality metrics"""
        quality_metrics = {}
        
        for column in df.columns:
            quality_metrics[column] = {
                'null_percentage': df[column].isnull().sum() / len(df) * 100,
                'unique_values': df[column].nunique(),
                'data_type': str(df[column].dtype),
                'completeness': (len(df) - df[column].isnull().sum()) / len(df) * 100
            }
            
            # Check for duplicates
            if df[column].dtype in ['object', 'string']:
                quality_metrics[column]['duplicate_percentage'] = df[column].duplicated().sum() / len(df) * 100
                
        return quality_metrics
    
    def _infer_column_types(self, df: pd.DataFrame) -> Dict[str, str]:
        """Infer semantic column types using AI"""
        column_types = {}
        
        for column in df.columns:
            sample_values = df[column].dropna().astype(str).head(100).tolist()
            column_types[column] = self._classify_column_type(column, sample_values)
            
        return column_types
    
    def _classify_column_type(self, column_name: str, sample_values: List[str]) -> str:
        """Classify column type using pattern matching and AI"""
        column_lower = column_name.lower()
        
        # Email pattern
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if any(re.match(email_pattern, val) for val in sample_values[:10]):
            return 'email'
            
        # Phone pattern
        phone_pattern = r'^\+?1?-?\.?\s?\(?[0-9]{3}\)?[\s.-]?[0-9]{3}[\s.-]?[0-9]{4}$'
        if any(re.match(phone_pattern, val) for val in sample_values[:10]):
            return 'phone'
            
        # Date pattern
        date_patterns = [r'\d{4}-\d{2}-\d{2}', r'\d{2}/\d{2}/\d{4}', r'\d{2}-\d{2}-\d{4}']
        if any(any(re.match(pattern, val) for pattern in date_patterns) for val in sample_values[:10]):
            return 'date'
            
        # ID patterns
        if any(keyword in column_lower for keyword in ['id', 'key', 'uuid']):
            return 'identifier'
            
        # Currency pattern
        if any(keyword in column_lower for keyword in ['price', 'cost', 'amount', 'salary']):
            return 'currency'
            
        # Geographic
        if any(keyword in column_lower for keyword in ['address', 'city', 'state', 'country', 'zip']):
            return 'geographic'
            
        return 'generic'
    
    def _detect_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Detect data patterns using ML"""
        patterns = {}
        
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        if len(numeric_columns) > 0:
            numeric_data = df[numeric_columns].fillna(0)
            
            if len(numeric_data) > 0:
                # Clustering to find patterns
                try:
                    scaled_data = self.scaler.fit_transform(numeric_data)
                    clusters = self.pattern_model.fit_predict(scaled_data)
                    patterns['clusters'] = {
                        'n_clusters': len(np.unique(clusters)),
                        'cluster_sizes': dict(zip(*np.unique(clusters, return_counts=True)))
                    }
                except:
                    patterns['clusters'] = {'error': 'Could not perform clustering'}
                    
        return patterns
    
    def _detect_anomalies(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Detect anomalies using Isolation Forest"""
        anomalies = {}
        
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        if len(numeric_columns) > 0:
            numeric_data = df[numeric_columns].fillna(0)
            
            if len(numeric_data) > 10:  # Need sufficient data for anomaly detection
                try:
                    outliers = self.anomaly_detector.fit_predict(numeric_data)
                    anomaly_count = np.sum(outliers == -1)
                    anomalies['numeric_anomalies'] = {
                        'count': int(anomaly_count),
                        'percentage': float(anomaly_count / len(df) * 100)
                    }
                except:
                    anomalies['numeric_anomalies'] = {'error': 'Could not detect anomalies'}
                    
        return anomalies
    
    def _analyze_correlations(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze correlations between numeric columns"""
        numeric_df = df.select_dtypes(include=[np.number])
        
        if len(numeric_df.columns) > 1:
            corr_matrix = numeric_df.corr()
            
            # Find strong correlations
            strong_correlations = []
            for i in range(len(corr_matrix.columns)):
                for j in range(i+1, len(corr_matrix.columns)):
                    corr_value = corr_matrix.iloc[i, j]
                    if abs(corr_value) > 0.7:  # Strong correlation threshold
                        strong_correlations.append({
                            'column1': corr_matrix.columns[i],
                            'column2': corr_matrix.columns[j],
                            'correlation': float(corr_value)
                        })
                        
            return {
                'strong_correlations': strong_correlations,
                'correlation_matrix': corr_matrix.to_dict()
            }
            
        return {'correlations': 'Insufficient numeric columns for correlation analysis'}
    
    def _generate_recommendations(self, analysis: Dict[str, Any], df: pd.DataFrame) -> List[str]:
        """Generate AI-powered recommendations"""
        recommendations = []
        
        # Data quality recommendations
        quality_data = analysis['data_quality']
        for column, metrics in quality_data.items():
            if metrics['null_percentage'] > 20:
                recommendations.append(f"High null percentage ({metrics['null_percentage']:.1f}%) in column '{column}'. Consider imputation or removal.")
                
            if 'duplicate_percentage' in metrics and metrics['duplicate_percentage'] > 10:
                recommendations.append(f"High duplicate percentage ({metrics['duplicate_percentage']:.1f}%) in column '{column}'. Consider deduplication.")
        
        # Type-based recommendations
        column_types = analysis['column_types']
        for column, col_type in column_types.items():
            if col_type == 'email':
                recommendations.append(f"Column '{column}' contains emails. Consider email validation and privacy measures.")
            elif col_type == 'identifier':
                recommendations.append(f"Column '{column}' appears to be an identifier. Consider indexing for performance.")
            elif col_type == 'date':
                recommendations.append(f"Column '{column}' contains dates. Consider date parsing and time-based analysis.")
        
        # Correlation recommendations
        if 'strong_correlations' in analysis['correlations']:
            for corr in analysis['correlations']['strong_correlations']:
                recommendations.append(f"Strong correlation ({corr['correlation']:.2f}) between '{corr['column1']}' and '{corr['column2']}'. Consider feature engineering.")
        
        # Anomaly recommendations
        if 'numeric_anomalies' in analysis['anomalies']:
            anomaly_pct = analysis['anomalies']['numeric_anomalies'].get('percentage', 0)
            if anomaly_pct > 5:
                recommendations.append(f"High anomaly rate ({anomaly_pct:.1f}%). Consider outlier handling strategies.")
        
        return recommendations

class PipelineGenerator:
    """AI-powered pipeline code generation"""
    
    def __init__(self):
        self.templates = self._load_templates()
    
    def _load_templates(self) -> Dict[str, str]:
        """Load code templates for different frameworks"""
        return {
            'airflow_dag': '''from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import logging

default_args = {{
    'owner': 'dataops-ai',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}}

dag = DAG(
    '{pipeline_name}',
    default_args=default_args,
    description='{description}',
    schedule_interval='{schedule}',
    catchup=False,
    tags=['ai-generated', 'dataops'],
)

def extract_data(**context):
    """Extract data from source"""
    {extract_code}

def transform_data(**context):
    """Transform data using AI recommendations"""
    {transform_code}

def validate_data(**context):
    """Validate data quality"""
    {validation_code}

def load_data(**context):
    """Load data to destination"""
    {load_code}

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
''',
            'dbt_model': '''-- AI-Generated dbt model for {pipeline_name}
-- Generated on: {timestamp}

{{{{ config(
    materialized='table',
    tags=['ai-generated', 'dataops']
) }}}}

WITH source_data AS (
    SELECT *
    FROM {{{{ source('{source_schema}', '{source_table}') }}}}
),

cleaned_data AS (
    SELECT
        {select_columns}
    FROM source_data
    WHERE {where_conditions}
),

transformed_data AS (
    SELECT
        *,
        {transformations}
    FROM cleaned_data
)

SELECT * FROM transformed_data

-- Data quality tests will be generated separately
''',
            'prefect_flow': '''from prefect import flow, task
import pandas as pd
from datetime import datetime
import logging

@task(name="extract_data")
def extract_data():
    """Extract data from source"""
    {extract_code}

@task(name="transform_data")
def transform_data(data):
    """Transform data using AI recommendations"""
    {transform_code}

@task(name="validate_data")
def validate_data(data):
    """Validate data quality"""
    {validation_code}

@task(name="load_data")
def load_data(data):
    """Load data to destination"""
    {load_code}

@flow(name="{pipeline_name}")
def {pipeline_name_clean}_flow():
    """AI-generated Prefect flow for {description}"""
    
    # Extract
    raw_data = extract_data()
    
    # Transform
    transformed_data = transform_data(raw_data)
    
    # Validate
    validated_data = validate_data(transformed_data)
    
    # Load
    load_data(validated_data)
    
    return "Pipeline completed successfully"

if __name__ == "__main__":
    {pipeline_name_clean}_flow()
'''
        }
    
    def generate_pipeline_code(self, config: PipelineConfig, analysis: Dict[str, Any]) -> Dict[str, str]:
        """Generate pipeline code based on configuration and AI analysis"""
        generated_code = {}
        
        # Generate extraction code
        extract_code = self._generate_extract_code(config.source_type, config.source_config)
        
        # Generate transformation code based on AI recommendations
        transform_code = self._generate_transform_code(config.transformations, analysis)
        
        # Generate validation code based on AI analysis
        validation_code = self._generate_validation_code(config.quality_checks, analysis)
        
        # Generate load code
        load_code = self._generate_load_code(config.destination_type, config.destination_config)
        
        # Clean pipeline name for Python identifiers
        pipeline_name_clean = config.name.replace(' ', '_').replace('-', '_').lower()
        
        # Generate framework-specific code
        if config.framework.lower() == 'airflow':
            generated_code['dag.py'] = self.templates['airflow_dag'].format(
                pipeline_name=pipeline_name_clean,
                description=f"AI-generated pipeline for {config.name}",
                schedule=config.schedule,
                extract_code=self._indent_code(extract_code, 4),
                transform_code=self._indent_code(transform_code, 4),
                validation_code=self._indent_code(validation_code, 4),
                load_code=self._indent_code(load_code, 4)
            )
        elif config.framework.lower() == 'dbt':
            generated_code['model.sql'] = self._generate_dbt_model(config, analysis)
        elif config.framework.lower() == 'prefect':
            generated_code['flow.py'] = self.templates['prefect_flow'].format(
                pipeline_name=config.name,
                pipeline_name_clean=pipeline_name_clean,
                description=f"AI-generated pipeline for {config.name}",
                extract_code=self._indent_code(extract_code, 4),
                transform_code=self._indent_code(transform_code, 4),
                validation_code=self._indent_code(validation_code, 4),
                load_code=self._indent_code(load_code, 4)
            )
        
        # Generate requirements.txt
        generated_code['requirements.txt'] = self._generate_requirements(config.framework)
        
        # Generate configuration files
        generated_code['config.yaml'] = yaml.dump(asdict(config), default_flow_style=False)
        
        return generated_code
    
    def _indent_code(self, code: str, spaces: int) -> str:
        """Indent code block by specified number of spaces"""
        lines = code.strip().split('\n')
        indented_lines = [' ' * spaces + line if line.strip() else line for line in lines]
        return '\n'.join(indented_lines)
    
    def _generate_extract_code(self, source_type: str, source_config: Dict[str, Any]) -> str:
        """Generate extraction code based on source type"""
        if source_type.lower() == 'postgresql':
            return f'''import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host="{source_config.get('host', 'localhost')}",
    database="{source_config.get('database', '')}",
    user="{source_config.get('user', '')}",
    password="{source_config.get('password', '')}"
)

query = "{source_config.get('query', 'SELECT * FROM table')}"
df = pd.read_sql(query, conn)
conn.close()
logging.info(f"Extracted {{len(df)}} rows from PostgreSQL")
return df'''
        elif source_type.lower() == 'csv':
            return f'''import pandas as pd

df = pd.read_csv("{source_config.get('file_path', '')}")
logging.info(f"Extracted {{len(df)}} rows from CSV")
return df'''
        elif source_type.lower() == 'api':
            return f'''import requests
import pandas as pd

response = requests.get("{source_config.get('url', '')}")
data = response.json()
df = pd.DataFrame(data)
logging.info(f"Extracted {{len(df)}} rows from API")
return df'''
        else:
            return '''# Add your custom extraction logic here
df = pd.DataFrame()  # placeholder
return df'''
    
    def _generate_transform_code(self, transformations: List[Dict[str, Any]], analysis: Dict[str, Any]) -> str:
        """Generate transformation code based on AI analysis"""
        transform_steps = ['# Start with the extracted data', 'df = context["task_instance"].xcom_pull(task_ids="extract_data")']
        
        # Add AI-recommended transformations
        for recommendation in analysis.get('recommendations', []):
            if 'null percentage' in recommendation.lower():
                column_name = recommendation.split("'")[1] if "'" in recommendation else "unknown_column"
                transform_steps.append(f"# Handle nulls in {column_name}")
                transform_steps.append(f"df['{column_name}'] = df['{column_name}'].fillna(df['{column_name}'].median())")
            
            elif 'duplicate percentage' in recommendation.lower():
                transform_steps.append("# Remove duplicates")
                transform_steps.append("df = df.drop_duplicates()")
        
        # Add user-defined transformations
        for transformation in transformations:
            if transformation['type'] == 'filter':
                transform_steps.append(f"# Filter: {transformation['condition']}")
                transform_steps.append(f"df = df.query('{transformation['condition']}')")
            elif transformation['type'] == 'aggregate':
                transform_steps.append(f"# Aggregate by {transformation['group_by']}")
                transform_steps.append(f"df = df.groupby('{transformation['group_by']}').agg({transformation['aggregations']})")
        
        if len(transform_steps) <= 2:
            transform_steps.append("# No specific transformations needed based on AI analysis")
        
        transform_steps.append('logging.info(f"Transformed data: {{len(df)}} rows")')
        transform_steps.append('return df')
        
        return '\n'.join(transform_steps)
    
    def _generate_validation_code(self, quality_checks: List[Dict[str, Any]], analysis: Dict[str, Any]) -> str:
        """Generate validation code based on AI analysis"""
        validation_steps = ['# Start with the transformed data', 'df = context["task_instance"].xcom_pull(task_ids="transform_data")']
        
        # Add AI-recommended validations
        column_types = analysis.get('column_types', {})
        for column, col_type in column_types.items():
            if col_type == 'email':
                validation_steps.append(f"# Validate email format in {column}")
                validation_steps.append(f"email_valid = df['{column}'].str.contains('@', na=False)")
                validation_steps.append(f"if not email_valid.all():")
                validation_steps.append(f"    logging.warning(f'Found {{(~email_valid).sum()}} invalid emails in {column}')")
            elif col_type == 'identifier':
                validation_steps.append(f"# Check for unique identifiers in {column}")
                validation_steps.append(f"if df['{column}'].nunique() != len(df):")
                validation_steps.append(f"    logging.warning(f'Non-unique identifiers found in {column}')")
        
        # Add user-defined quality checks
        for check in quality_checks:
            if check['type'] == 'not_null':
                validation_steps.append(f"# Check for nulls in {check['column']}")
                validation_steps.append(f"null_count = df['{check['column']}'].isnull().sum()")
                validation_steps.append(f"if null_count > 0:")
                validation_steps.append(f"    raise ValueError(f'Found {{null_count}} null values in {check['column']}')")
            elif check['type'] == 'range':
                validation_steps.append(f"# Validate range for {check['column']}")
                validation_steps.append(f"out_of_range = df[(df['{check['column']}'] < {check['min']}) | (df['{check['column']}'] > {check['max']})].shape[0]")
                validation_steps.append(f"if out_of_range > 0:")
                validation_steps.append(f"    raise ValueError(f'Found {{out_of_range}} values out of range in {check['column']}')")
        
        if len(validation_steps) <= 2:
            validation_steps.append("# No specific validations configured")
        
        validation_steps.append("logging.info('Data validation completed successfully')")
        validation_steps.append('return df')
        
        return '\n'.join(validation_steps)
    
    def _generate_load_code(self, dest_type: str, dest_config: Dict[str, Any]) -> str:
        """Generate load code based on destination type"""
        if dest_type.lower() == 'postgresql':
            return f'''from sqlalchemy import create_engine

df = context["task_instance"].xcom_pull(task_ids="validate_data")
engine = create_engine('postgresql://{dest_config.get("user", "")}:{dest_config.get("password", "")}@{dest_config.get("host", "localhost")}/{dest_config.get("database", "")}')
df.to_sql('{dest_config.get("table", "processed_data")}', engine, if_exists='replace', index=False)
logging.info(f"Loaded {{len(df)}} rows to PostgreSQL")'''
        elif dest_type.lower() == 'csv':
            return f'''df = context["task_instance"].xcom_pull(task_ids="validate_data")
df.to_csv('{dest_config.get("file_path", "output.csv")}', index=False)
logging.info(f"Loaded {{len(df)}} rows to CSV")'''
        else:
            return '''# Add your custom load logic here
df = context["task_instance"].xcom_pull(task_ids="validate_data")
logging.info(f"Custom load logic for {{len(df)}} rows")'''
    
    def _generate_dbt_model(self, config: PipelineConfig, analysis: Dict[str, Any]) -> str:
        """Generate dbt model SQL"""
        return self.templates['dbt_model'].format(
            pipeline_name=config.name.replace(' ', '_').lower(),
            timestamp=datetime.now().isoformat(),
            source_schema=config.source_config.get('schema', 'public'),
            source_table=config.source_config.get('table', 'source_table'),
            select_columns="*",
            where_conditions="1=1",
            transformations="-- AI-recommended transformations would go here"
        )
    
    def _generate_requirements(self, framework: str) -> str:
        """Generate requirements.txt based on framework"""
        base_requirements = [
            "pandas>=1.3.0",
            "numpy>=1.21.0",
            "scikit-learn>=1.0.0",
            "pyyaml>=6.0",
            "requests>=2.28.0"
        ]
        
        if framework.lower() == 'airflow':
            base_requirements.extend([
                "apache-airflow>=2.5.0",
                "psycopg2-binary>=2.9.0"
            ])
        elif framework.lower() == 'prefect':
            base_requirements.extend([
                "prefect>=2.0.0"
            ])
        elif framework.lower() == 'dbt':
            base_requirements.extend([
                "dbt-core>=1.0.0",
                "dbt-postgres>=1.0.0"
            ])
        
        return '\n'.join(base_requirements)

class DataQualityValidator:
    """AI-powered data quality validation"""
    
    def __init__(self):
        self.anomaly_detector = IsolationForest(contamination=0.1, random_state=42)
        
    def validate_data_quality(self, df: pd.DataFrame, quality_rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate data quality using AI and predefined rules"""
        validation_results = {
            'passed': True,
            'issues': [],
            'metrics': {},
            'ai_insights': {}
        }
        
        # Run predefined quality checks
        for rule in quality_rules:
            result = self._execute_quality_rule(df, rule)
            if not result['passed']:
                validation_results['passed'] = False
                validation_results['issues'].append(result)
        
        # AI-powered quality assessment
        ai_insights = self._ai_quality_assessment(df)
        validation_results['ai_insights'] = ai_insights
        
        # Overall metrics
        validation_results['metrics'] = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'completeness_score': self._calculate_completeness_score(df),
            'consistency_score': self._calculate_consistency_score(df),
            'accuracy_score': ai_insights.get('accuracy_score', 0.95)
        }
        
        return validation_results
    
    def _execute_quality_rule(self, df: pd.DataFrame, rule: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single quality rule"""
        try:
            if rule['type'] == 'not_null':
                null_count = df[rule['column']].isnull().sum()
                passed = null_count == 0
                return {
                    'rule': rule,
                    'passed': passed,
                    'message': f"Found {null_count} null values in {rule['column']}" if not passed else "No null values found"
                }
            
            elif rule['type'] == 'unique':
                duplicate_count = df[rule['column']].duplicated().sum()
                passed = duplicate_count == 0
                return {
                    'rule': rule,
                    'passed': passed,
                    'message': f"Found {duplicate_count} duplicate values in {rule['column']}" if not passed else "All values are unique"
                }
            
            elif rule['type'] == 'range':
                out_of_range = df[(df[rule['column']] < rule['min']) | (df[rule['column']] > rule['max'])].shape[0]
                passed = out_of_range == 0
                return {
                    'rule': rule,
                    'passed': passed,
                    'message': f"Found {out_of_range} values out of range [{rule['min']}, {rule['max']}] in {rule['column']}" if not passed else "All values in range"
                }
            
            else:
                return {
                    'rule': rule,
                    'passed': False,
                    'message': f"Unknown rule type: {rule['type']}"
                }
                
        except Exception as e:
            return {
                'rule': rule,
                'passed': False,
                'message': f"Error executing rule: {str(e)}"
            }
    
    def _ai_quality_assessment(self, df: pd.DataFrame) -> Dict[str, Any]:
        """AI-powered quality assessment"""
        insights = {}
        
        # Detect anomalies in numeric columns
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        if len(numeric_columns) > 0:
            numeric_data = df[numeric_columns].fillna(0)
            
            if len(numeric_data) > 10:
                try:
                    outliers = self.anomaly_detector.fit_predict(numeric_data)
                    anomaly_percentage = (outliers == -1).sum() / len(df) * 100
                    insights['anomaly_detection'] = {
                        'anomaly_percentage': float(anomaly_percentage),
                        'anomaly_count': int((outliers == -1).sum()),
                        'status': 'high' if anomaly_percentage > 5 else 'normal'
                    }
                except:
                    insights['anomaly_detection'] = {'error': 'Could not perform anomaly detection'}
        
        # Pattern consistency analysis
        insights['pattern_consistency'] = self._analyze_pattern_consistency(df)
        
        # Data freshness analysis
        insights['data_freshness'] = self._analyze_data_freshness(df)
        
        # Calculate overall accuracy score based on various factors
        accuracy_factors = []
        if 'anomaly_detection' in insights and 'anomaly_percentage' in insights['anomaly_detection']:
            anomaly_score = max(0, 1 - (insights['anomaly_detection']['anomaly_percentage'] / 100))
            accuracy_factors.append(anomaly_score)
        
        if accuracy_factors:
            insights['accuracy_score'] = sum(accuracy_factors) / len(accuracy_factors)
        else:
            insights['accuracy_score'] = 0.95
            
        return insights
    
    def _analyze_pattern_consistency(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze pattern consistency across columns"""
        consistency_results = {}
        
        for column in df.columns:
            if df[column].dtype == 'object':
                # Check string pattern consistency
                patterns = df[column].dropna().astype(str).apply(lambda x: len(x)).value_counts()
                if len(patterns) > 1:
                    consistency_score = patterns.max() / patterns.sum()
                    consistency_results[column] = {
                        'type': 'string_length_pattern',
                        'consistency_score': float(consistency_score),
                        'status': 'inconsistent' if consistency_score < 0.8 else 'consistent'
                    }
        
        return consistency_results
    
    def _analyze_data_freshness(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze data freshness based on date columns"""
        freshness_results = {}
        
        # Try to identify date columns
        date_columns = []
        for column in df.columns:
            if 'date' in column.lower() or 'time' in column.lower():
                try:
                    pd.to_datetime(df[column].head(100))
                    date_columns.append(column)
                except:
                    continue
        
        if date_columns:
            for column in date_columns:
                try:
                    dates = pd.to_datetime(df[column].dropna())
                    if len(dates) > 0:
                        latest_date = dates.max()
                        days_old = (datetime.now() - latest_date).days
                        freshness_results[column] = {
                            'latest_date': latest_date.isoformat(),
                            'days_old': int(days_old),
                            'status': 'fresh' if days_old <= 7 else 'stale'
                        }
                except:
                    continue
        
        return freshness_results
    
    def _calculate_completeness_score(self, df: pd.DataFrame) -> float:
        """Calculate overall data completeness score"""
        total_cells = df.shape[0] * df.shape[1]
        missing_cells = df.isnull().sum().sum()
        return float((total_cells - missing_cells) / total_cells)
    
    def _calculate_consistency_score(self, df: pd.DataFrame) -> float:
        """Calculate overall data consistency score"""
        consistency_scores = []
        
        for column in df.columns:
            if df[column].dtype == 'object':
                # Check for consistent formatting
                unique_patterns = df[column].dropna().astype(str).apply(lambda x: len(x.strip())).nunique()
                if unique_patterns <= 3:  # Arbitrary threshold for "consistent"
                    consistency_scores.append(1.0)
                else:
                    consistency_scores.append(0.7)
            else:
                # Numeric columns are generally consistent
                consistency_scores.append(1.0)
        
        return sum(consistency_scores) / len(consistency_scores) if consistency_scores else 1.0

class DataOpsManager:
    """Main DataOps manager for coordinating AI-powered pipeline operations"""
    
    def __init__(self, config_path: Optional[str] = None):
        self.analyzer = AIDataAnalyzer()
        self.generator = PipelineGenerator()
        self.validator = DataQualityValidator()
        self.pipelines: Dict[str, PipelineConfig] = {}
        self.config_path = config_path or "dataops_config.json"
        
        # Load existing configurations
        self._load_configurations()
    
    def create_pipeline_from_data(self, 
                                 data_source: str, 
                                 pipeline_name: str,
                                 framework: str = "airflow",
                                 destination_config: Dict[str, Any] = None) -> Dict[str, Any]:
        """Create a complete pipeline from data source using AI analysis"""
        
        logger.info(f"Creating AI-powered pipeline: {pipeline_name}")
        
        # Step 1: Load and analyze data
        try:
            if data_source.endswith('.csv'):
                df = pd.read_csv(data_source)
            elif data_source.endswith('.json'):
                df = pd.read_json(data_source)
            else:
                raise ValueError(f"Unsupported data source format: {data_source}")
                
            logger.info(f"Loaded data with shape: {df.shape}")
            
        except Exception as e:
            logger.error(f"Failed to load data: {str(e)}")
            return {"error": f"Failed to load data: {str(e)}"}
        
        # Step 2: AI-powered data analysis
        logger.info("Performing AI analysis on data...")
        analysis_results = self.analyzer.analyze_data_patterns(df)
        
        # Step 3: Generate pipeline configuration based on AI insights
        pipeline_config = self._create_pipeline_config(
            pipeline_name, 
            data_source, 
            analysis_results, 
            framework,
            destination_config or {"type": "csv", "file_path": "output.csv"}
        )
        
        # Step 4: Generate pipeline code
        logger.info("Generating pipeline code...")
        generated_code = self.generator.generate_pipeline_code(pipeline_config, analysis_results)
        
        # Step 5: Save pipeline configuration
        self.pipelines[pipeline_name] = pipeline_config
        self._save_configurations()
        
        return {
            "success": True,
            "pipeline_name": pipeline_name,
            "analysis": analysis_results,
            "config": asdict(pipeline_config),
            "generated_code": generated_code,
            "recommendations": analysis_results.get('recommendations', [])
        }
    
    def _create_pipeline_config(self, 
                               name: str, 
                               data_source: str, 
                               analysis: Dict[str, Any],
                               framework: str,
                               destination_config: Dict[str, Any]) -> PipelineConfig:
        """Create pipeline configuration based on AI analysis"""
        
        # Determine source configuration
        if data_source.endswith('.csv'):
            source_config = {"file_path": data_source}
            source_type = "csv"
        elif data_source.endswith('.json'):
            source_config = {"file_path": data_source}
            source_type = "json"
        else:
            source_config = {"connection_string": data_source}
            source_type = "database"
        
        # Generate transformations based on AI recommendations
        transformations = []
        for recommendation in analysis.get('recommendations', []):
            if 'null percentage' in recommendation.lower():
                column_name = recommendation.split("'")[1] if "'" in recommendation else "unknown_column"
                transformations.append({
                    "type": "fill_nulls",
                    "column": column_name,
                    "method": "median"
                })
            elif 'duplicate' in recommendation.lower():
                transformations.append({
                    "type": "remove_duplicates",
                    "subset": None
                })
        
        # Generate quality checks based on AI analysis
        quality_checks = []
        column_types = analysis.get('column_types', {})
        for column, col_type in column_types.items():
            if col_type == 'email':
                quality_checks.append({
                    "type": "email_validation",
                    "column": column
                })
            elif col_type == 'identifier':
                quality_checks.append({
                    "type": "unique",
                    "column": column
                })
        
        # Determine schedule based on data freshness
        freshness_info = analysis.get('ai_insights', {}).get('data_freshness', {})
        if freshness_info:
            avg_days_old = sum(info.get('days_old', 7) for info in freshness_info.values()) / len(freshness_info)
            if avg_days_old <= 1:
                schedule = "@hourly"
            elif avg_days_old <= 7:
                schedule = "@daily"
            else:
                schedule = "@weekly"
        else:
            schedule = "@daily"
        
        return PipelineConfig(
            name=name,
            source_type=source_type,
            source_config=source_config,
            transformations=transformations,
            destination_type=destination_config.get("type", "csv"),
            destination_config=destination_config,
            schedule=schedule,
            quality_checks=quality_checks,
            framework=framework
        )
    
    def validate_pipeline_data(self, pipeline_name: str, data_path: str) -> Dict[str, Any]:
        """Validate data for a specific pipeline"""
        
        if pipeline_name not in self.pipelines:
            return {"error": f"Pipeline '{pipeline_name}' not found"}
        
        try:
            # Load data
            df = pd.read_csv(data_path) if data_path.endswith('.csv') else pd.read_json(data_path)
            
            # Get pipeline configuration
            pipeline_config = self.pipelines[pipeline_name]
            
            # Perform validation
            validation_results = self.validator.validate_data_quality(df, pipeline_config.quality_checks)
            
            return validation_results
            
        except Exception as e:
            return {"error": f"Validation failed: {str(e)}"}
    
    def get_pipeline_status(self, pipeline_name: str) -> Dict[str, Any]:
        """Get status and metrics for a pipeline"""
        
        if pipeline_name not in self.pipelines:
            return {"error": f"Pipeline '{pipeline_name}' not found"}
        
        config = self.pipelines[pipeline_name]
        
        return {
            "name": config.name,
            "framework": config.framework,
            "schedule": config.schedule,
            "created_at": config.created_at.isoformat(),
            "source_type": config.source_type,
            "destination_type": config.destination_type,
            "transformations_count": len(config.transformations),
            "quality_checks_count": len(config.quality_checks)
        }
    
    def list_pipelines(self) -> List[Dict[str, Any]]:
        """List all configured pipelines"""
        return [self.get_pipeline_status(name) for name in self.pipelines.keys()]
    
    def update_pipeline(self, pipeline_name: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing pipeline configuration"""
        
        if pipeline_name not in self.pipelines:
            return {"error": f"Pipeline '{pipeline_name}' not found"}
        
        try:
            config = self.pipelines[pipeline_name]
            
            # Update allowed fields
            if 'schedule' in updates:
                config.schedule = updates['schedule']
            if 'transformations' in updates:
                config.transformations = updates['transformations']
            if 'quality_checks' in updates:
                config.quality_checks = updates['quality_checks']
            
            # Save updated configuration
            self._save_configurations()
            
            return {
                "success": True,
                "message": f"Pipeline '{pipeline_name}' updated successfully",
                "config": asdict(config)
            }
            
        except Exception as e:
            return {"error": f"Failed to update pipeline: {str(e)}"}
    
    def delete_pipeline(self, pipeline_name: str) -> Dict[str, Any]:
        """Delete a pipeline configuration"""
        
        if pipeline_name not in self.pipelines:
            return {"error": f"Pipeline '{pipeline_name}' not found"}
        
        del self.pipelines[pipeline_name]
        self._save_configurations()
        
        return {
            "success": True,
            "message": f"Pipeline '{pipeline_name}' deleted successfully"
        }
    
    def generate_pipeline_documentation(self, pipeline_name: str) -> str:
        """Generate comprehensive documentation for a pipeline"""
        
        if pipeline_name not in self.pipelines:
            return f"Error: Pipeline '{pipeline_name}' not found"
        
        config = self.pipelines[pipeline_name]
        
        doc = f"""# {config.name} Pipeline Documentation

## Overview
- **Framework**: {config.framework}
- **Created**: {config.created_at.strftime('%Y-%m-%d %H:%M:%S')}
- **Schedule**: {config.schedule}

## Data Source
- **Type**: {config.source_type}
- **Configuration**: {json.dumps(config.source_config, indent=2)}

## Data Destination
- **Type**: {config.destination_type}
- **Configuration**: {json.dumps(config.destination_config, indent=2)}

## Transformations ({len(config.transformations)})
"""
        
        for i, transform in enumerate(config.transformations, 1):
            doc += f"### {i}. {transform.get('type', 'Unknown').title()}\n"
            for key, value in transform.items():
                if key != 'type':
                    doc += f"- **{key.title()}**: {value}\n"
            doc += "\n"
        
        doc += f"""## Quality Checks ({len(config.quality_checks)})
"""
        
        for i, check in enumerate(config.quality_checks, 1):
            doc += f"### {i}. {check.get('type', 'Unknown').title()}\n"
            for key, value in check.items():
                if key != 'type':
                    doc += f"- **{key.title()}**: {value}\n"
            doc += "\n"
        
        return doc
    
    def _load_configurations(self):
        """Load pipeline configurations from file"""
        try:
            if Path(self.config_path).exists():
                with open(self.config_path, 'r') as f:
                    configs = json.load(f)
                    
                for name, config_data in configs.items():
                    # Convert datetime string back to datetime object
                    if 'created_at' in config_data:
                        config_data['created_at'] = datetime.fromisoformat(config_data['created_at'])
                    
                    self.pipelines[name] = PipelineConfig(**config_data)
                    
                logger.info(f"Loaded {len(self.pipelines)} pipeline configurations")
        except Exception as e:
            logger.warning(f"Could not load configurations: {str(e)}")
    
    def _save_configurations(self):
        """Save pipeline configurations to file"""
        try:
            configs = {}
            for name, config in self.pipelines.items():
                config_dict = asdict(config)
                # Convert datetime to string for JSON serialization
                if 'created_at' in config_dict:
                    config_dict['created_at'] = config_dict['created_at'].isoformat()
                configs[name] = config_dict
            
            with open(self.config_path, 'w') as f:
                json.dump(configs, f, indent=2)
                
            logger.info(f"Saved {len(self.pipelines)} pipeline configurations")
        except Exception as e:
            logger.error(f"Could not save configurations: {str(e)}")

class DataOpsAPI:
    """REST API interface for DataOps operations"""
    
    def __init__(self, manager: DataOpsManager):
        self.manager = manager
    
    def create_pipeline_endpoint(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """API endpoint for creating pipelines"""
        required_fields = ['data_source', 'pipeline_name']
        
        for field in required_fields:
            if field not in request_data:
                return {"error": f"Missing required field: {field}"}
        
        return self.manager.create_pipeline_from_data(
            data_source=request_data['data_source'],
            pipeline_name=request_data['pipeline_name'],
            framework=request_data.get('framework', 'airflow'),
            destination_config=request_data.get('destination_config')
        )
    
    def validate_data_endpoint(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """API endpoint for data validation"""
        required_fields = ['pipeline_name', 'data_path']
        
        for field in required_fields:
            if field not in request_data:
                return {"error": f"Missing required field: {field}"}
        
        return self.manager.validate_pipeline_data(
            pipeline_name=request_data['pipeline_name'],
            data_path=request_data['data_path']
        )
    
    def get_pipeline_endpoint(self, pipeline_name: str) -> Dict[str, Any]:
        """API endpoint for getting pipeline status"""
        return self.manager.get_pipeline_status(pipeline_name)
    
    def list_pipelines_endpoint(self) -> Dict[str, Any]:
        """API endpoint for listing all pipelines"""
        return {"pipelines": self.manager.list_pipelines()}
    
    def update_pipeline_endpoint(self, pipeline_name: str, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """API endpoint for updating pipelines"""
        return self.manager.update_pipeline(pipeline_name, request_data)
    
    def delete_pipeline_endpoint(self, pipeline_name: str) -> Dict[str, Any]:
        """API endpoint for deleting pipelines"""
        return self.manager.delete_pipeline(pipeline_name)

# Example usage and demo functions
def demo_ai_pipeline_creation():
    """Demonstrate AI-powered pipeline creation"""
    
    # Create sample data
    sample_data = {
        'customer_id': range(1000, 2000),
        'email': [f'user{i}@example.com' for i in range(1000)],
        'purchase_amount': np.random.normal(100, 25, 1000),
        'purchase_date': pd.date_range('2024-01-01', periods=1000, freq='1H'),
        'product_category': np.random.choice(['Electronics', 'Clothing', 'Books', 'Home'], 1000),
        'customer_age': np.random.randint(18, 80, 1000)
    }
    
    df = pd.DataFrame(sample_data)
    
    # Add some data quality issues for demonstration
    df.loc[50:60, 'email'] = None  # Missing emails
    df.loc[100:105, 'customer_id'] = 1001  # Duplicate IDs
    df.loc[200, 'purchase_amount'] = 10000  # Outlier
    
    # Save sample data
    df.to_csv('sample_customer_data.csv', index=False)
    
    # Create DataOps manager
    manager = DataOpsManager()
    
    # Create AI-powered pipeline
    print("Creating AI-powered data pipeline...")
    result = manager.create_pipeline_from_data(
        data_source='sample_customer_data.csv',
        pipeline_name='customer_analytics_pipeline',
        framework='airflow',
        destination_config={
            'type': 'postgresql',
            'host': 'localhost',
            'database': 'analytics',
            'table': 'processed_customers'
        }
    )
    
    if result.get('success'):
        print(f"✅ Pipeline created successfully!")
        print(f"📊 Analysis found {len(result['recommendations'])} recommendations:")
        for rec in result['recommendations']:
            print(f"   • {rec}")
        
        print(f"\n🔧 Generated files:")
        for filename in result['generated_code'].keys():
            print(f"   • {filename}")
        
        # Save generated files
        for filename, content in result['generated_code'].items():
            with open(f"generated_{filename}", 'w') as f:
                f.write(content)
        
        print(f"\n📋 Pipeline documentation:")
        doc = manager.generate_pipeline_documentation('customer_analytics_pipeline')
        print(doc[:500] + "..." if len(doc) > 500 else doc)
        
    else:
        print(f"❌ Pipeline creation failed: {result.get('error')}")

def demo_data_validation():
    """Demonstrate AI-powered data validation"""
    
    manager = DataOpsManager()
    
    # Validate the sample data
    print("\nValidating pipeline data...")
    validation_result = manager.validate_pipeline_data(
        pipeline_name='customer_analytics_pipeline',
        data_path='sample_customer_data.csv'
    )
    
    if 'error' not in validation_result:
        print(f"📊 Data Quality Metrics:")
        metrics = validation_result.get('metrics', {})
        for metric, value in metrics.items():
            print(f"   • {metric.replace('_', ' ').title()}: {value:.2f}")
        
        if validation_result.get('issues'):
            print(f"\n⚠️  Data Quality Issues Found:")
            for issue in validation_result['issues']:
                print(f"   • {issue['message']}")
        else:
            print(f"\n✅ No data quality issues found!")
            
        # AI insights
        ai_insights = validation_result.get('ai_insights', {})
        if 'anomaly_detection' in ai_insights:
            anomaly_info = ai_insights['anomaly_detection']
            print(f"\n🤖 AI Insights:")
            print(f"   • Anomaly Detection: {anomaly_info.get('anomaly_percentage', 0):.1f}% anomalies detected")
    else:
        print(f"❌ Validation failed: {validation_result['error']}")

if __name__ == "__main__":
    print("🚀 DataOps: AI-Powered Automated Data Pipeline Builder")
    print("=" * 60)
    
    # Run demonstrations
    demo_ai_pipeline_creation()
    demo_data_validation()
    
    print("\n✨ DataOps AI Pipeline Builder demo completed!")
    print("Generated files are saved with 'generated_' prefix")
    print("Check generated_dag.py for your Airflow pipeline!")