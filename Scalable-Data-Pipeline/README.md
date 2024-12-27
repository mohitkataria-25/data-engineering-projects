
# Airflow Ingestion Pipeline DAG

This repository contains an Airflow DAG for an ingestion pipeline that fetches data from Mockaroo, processes it locally, and uploads it to an S3 bucket.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
- [DAG Workflow](#dag-workflow)
- [Usage](#usage)
- [Configuration](#configuration)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

The `ingestion_pipeline_dag` automates the process of fetching synthetic data from [Mockaroo](https://www.mockaroo.com/), stores it locally, and then uploads the data to an S3 bucket. The pipeline is designed to handle multiple datasets and supports modular schema definitions.

---

## Features

- Fetch synthetic data from Mockaroo.
- Dynamically create an S3 bucket if it does not already exist.
- Store data locally for backup.
- Upload data to a defined S3 bucket.
- Modular and reusable Python scripts.
- Fully automated DAG with Airflow scheduling.

---

## Prerequisites

Before running the DAG, ensure you have the following:

1. **Airflow**: Installed and configured on your local machine or server.
2. **AWS Account**: Credentials (Access Key and Secret Key) for accessing S3.
3. **Mockaroo API Key**: For generating synthetic data.
4. **Python Packages**: Install dependencies listed in the `requirements.txt` file.

---

## Setup Instructions

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/yourusername/ingestion-pipeline-dag.git
   cd ingestion-pipeline-dag
   ```

2. **Install Dependencies**:
   Create a virtual environment and install the required packages.
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Configure Airflow**:
   - Set up your Airflow environment.
   - Copy the DAG file (`ingestion_pipeline_dag.py`) to your Airflow DAGs folder.

   ```bash
   cp ingestion_pipeline_dag.py ~/airflow/dags/
   ```

4. **Set Up Environment Variables**:
   Ensure that the following environment variables are set or configure them in the DAG file:
   - `AWS_ACCESS_KEY`
   - `AWS_SECRET_KEY`
   - `MOCKAROO_API_KEY`
   - `S3_BUCKET_NAME`
   - `LOCAL_STORAGE_PATH`

5. **Start Airflow**:
   Start the Airflow scheduler and webserver.
   ```bash
   airflow scheduler &
   airflow webserver &
   ```

6. **Trigger the DAG**:
   Trigger the DAG manually via the Airflow web interface or CLI.
   ```bash
   airflow dags trigger ingestion_pipeline_dag
   ```

---

## DAG Workflow

The DAG consists of the following tasks:

1. **Define Parameters**: Sets up global parameters such as API keys, AWS credentials, and folder paths.
2. **Create S3 Bucket**: Creates the specified S3 bucket if it does not already exist.
3. **Fetch Mockaroo Data**: Fetches synthetic data from Mockaroo using pre-defined schemas.
4. **Ingest Data**: Processes, stores locally, and uploads the data to the S3 bucket.

### Task Dependencies

```plaintext
define_parameters_task >> create_s3_bucket_task >> ingest_data_task
```

---

## Usage

### Running the DAG

To run the DAG, either trigger it manually or let it run based on the schedule interval (default: hourly).

#### Trigger Manually
```bash
airflow dags trigger ingestion_pipeline_dag
```

#### Ad-hoc Task Execution
Run individual tasks using:
```bash
airflow tasks run ingestion_pipeline_dag <task_id> <execution_date>
```

---

## Configuration

The DAG accepts the following parameters:

| Parameter         | Description                           | Default Value                                  |
|-------------------|---------------------------------------|-----------------------------------------------|
| `MOCKAROO_API_KEY` | API key for Mockaroo                 | Replace in DAG file                           |
| `AWS_ACCESS_KEY`   | AWS Access Key                      | Replace in DAG file                           |
| `AWS_SECRET_KEY`   | AWS Secret Key                      | Replace in DAG file                           |
| `BUCKET_NAME`      | S3 bucket name                      | `ecommerce-ingestion-data-pipeline`          |
| `LOCAL_STORAGE`    | Local storage path for backups      | `./mockaroo_data`                             |

---

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for improvements, bug fixes, or new features.

---

## License

This project is licensed under the [MIT License](LICENSE).

---

## Author

[Your Name](https://github.com/yourusername)  
Email: your.email@example.com  

---

