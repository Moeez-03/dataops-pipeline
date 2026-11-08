# DataOps Pipeline Builder 🚀

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10+-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow-017CEE.svg?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![scikit-learn](https://img.shields.io/badge/ML-scikit--learn-F7931E.svg?logo=scikit-learn&logoColor=white)](https://scikit-learn.org/)

An automated end-to-end framework and visual UI for designing, generating, and orchestrating intelligent data pipelines with embedded machine learning and Apache Airflow DAG compilation.

---

## Overview

Modern data operations require bridging the gap between raw data ingestion, exploratory machine learning, and production orchestration. **DataOps Pipeline Builder** provides:

1. **Interactive Visual Designer**: Web canvas to construct pipeline stages (extract, transform, train/predict, alert, load).
2. **AI & ML Processing Engine**: Outlier detection (IsolationForest), unsupervised clustering (KMeans), feature scaling (StandardScaler), and regression analysis.
3. **Automated Airflow DAG Compiler**: Compiles high-level pipeline definitions into deployable, production-ready Apache Airflow DAG scripts and declarative YAML configs.
4. **Data Quality & Validation**: Built-in automated schema checking, null audits, and anomaly scoring.

---

## Architecture

`
┌─────────────────────────────────┐
│     Visual Pipeline Canvas      │  (Interactive HTML5 / UI)
└────────────────┬────────────────┘
                 │ Exports JSON / Config
                 ▼
┌─────────────────────────────────┐
│     DataOps Core Engine         │  (main1.py / Python Framework)
│  • Schema Profiling             │
│  • Outlier Detection (IsoForest)│
│  • Clustering & Normalization   │
└────────────────┬────────────────┘
                 │ Compiles to
         ┌───────┴────────┐
         ▼                ▼
┌────────────────┐  ┌─────────────────────────┐
│ generated.yaml │  │ generated_dag.py (Airflow)│
└────────────────┘  └─────────────────────────┘
`

---

## Features

- **Automated Airflow Compilation**: Generates Python scripts compatible with Apache Airflow 2.x with retry logic, scheduling, and error handlers.
- **Embedded Anomaly Detection**: Flags corrupted, skewed, or fraudulent incoming records using unsupervised isolation forests before loading into data warehouses.
- **Declarative YAML & JSON State**: Export and import complete pipeline blueprints for version-controlled GitOps workflows.
- **Zero-Friction Ingestion**: Built-in adapters for CSV, tabular data, and SQLite datastores.

---

## Project Structure

`	ext
dataops-pipeline/
├── index.html                           # Visual drag-and-drop pipeline designer UI
├── LICENSE                              # MIT License
├── README.md                            # Project documentation
└── pipline-builder/
    ├── main1.py                         # Core pipeline engine & Airflow generator
    ├── customer-analyte.py              # ML analytics & outlier detection module
    ├── dataops_config.json              # Sample pipeline configuration schema
    ├── generated_dag.py                 # Compiled Apache Airflow DAG output
    ├── generated_config.yaml            # Compiled YAML pipeline specification
    ├── generated_requirements.txt       # Dependencies for pipeline runtimes
    └── sample_customer_data.csv         # Benchmark dataset for validation
`

---

## Quickstart

### 1. Prerequisites
- Python 3.10 or higher
- Git

### 2. Setup Virtual Environment
`ash
git clone https://github.com/Moeez-03/dataops-pipeline.git
cd dataops-pipeline/pipline-builder
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r generated_requirements.txt
`

### 3. Visual Pipeline Designer
Open index.html in any modern web browser to interactively configure data sources, ML transformations, and export pipeline configurations.

### 4. Run Pipeline Generator & ML Analytics
`ash
# Execute the pipeline generator
python main1.py

# Run customer analytics and anomaly detection
python customer-analyte.py
`

Generated outputs will be written to generated_dag.py and generated_config.yaml.

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.