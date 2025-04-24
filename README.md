# 🎧 Sparkify Cloud-Native Data Pipeline

This project builds a scalable, cloud-native data pipeline for a fictional music streaming company, **Sparkify**, using AWS and Apache Spark. It extracts raw JSON data from S3, transforms it with PySpark, and loads it into a structured **data lake** in Parquet format.

---

## 📌 Project Goals

- Build a modern data lake architecture
- Process semi-structured data with Apache Spark
- Store transformed data in AWS S3 as Parquet
- Lay foundation for orchestration (Airflow), querying (Athena/Redshift), and containerization (Docker)

---

## ✅ Phase 1: Plan the Architecture

### 🎯 What & Why

Cloud-native, serverless, and cost-efficient:
- **S3** for raw & transformed data (acts as the data lake)
- **Spark (on local or EMR)** for distributed processing
- **Parquet** for optimized columnar storage
- Optional: Redshift, Athena, or Quicksight for analytics

### 🗂️ Folder Structure
```bash
Sparkify_Cloud_Native-pipeline/
│
├── raw_data/                      # Raw input data
│   ├── song_data/                 # Raw song data (JSON format)
│   └── log_data/                  # Raw log data (JSON format)
│
├── spark_jobs/                    # PySpark ETL job files
│   ├── spark_etl.py               # The PySpark ETL job that loads, transforms, and writes data
│   └── other_etl_jobs.py          # (Optional) Other Spark jobs (e.g., batch processing, transformations)
│
├── output/                        # (Optional) Local output storage for Parquet files (for testing)
│   ├── users/                     # Parquet files for the users table
│   ├── songs/                     # Parquet files for the songs table
│   ├── artists/                   # Parquet files for the artists table
│   └── songplays/                 # Parquet files for the songplays table
│
├── s3/                            # Folder to store S3 configurations (if necessary)
│   ├── s3_config.json             # Configuration for S3 bucket setup
│   └── dwh.cfg                    # AWS configuration file (with credentials and S3 bucket info)
│
│
├── Docker/          
│
├── scripts/                        # Utility or helper scripts for running the project
│   ├── run_etl.sh                 # Shell script to execute the PySpark ETL job (locally or remotely)
│   └── deploy.sh                  # (Optional) Script to deploy the project on AWS or other platforms
│
├── README.md                      # Project documentation
└── requirements.txt               # Python dependencies (e.g., pyspark, boto3, pandas)
```


---

## ✅ Phase 2: Initialize the Project

### 🔧 Setup

1. ✅ Create and connect GitHub repo  
2. ✅ Configure AWS CLI  
3. ✅ Set up `dwh.cfg` with:
   - AWS credentials
   - S3 bucket paths
   - Region

### 🪣 S3 Buckets Used

| Bucket Name                 | Purpose                |
|-----------------------------|------------------------|
| `sparkify-datalake-aws`     | Raw and processed data |
| `sparkify-datalake-athena-results` | Athena output (optional) |

### 📁 S3 Structure


s3://sparkify-datalake-aws/
``` bash 
├── raw/
│   ├── song_data/                 # Raw song data (JSON format)
│   └── log_data/                  # Raw log data (JSON format)
│
├── curated/                        # Processed/Transformed Data
│   ├── songs/                     # Processed song data (Parquet format)
│   ├── users/                     # Processed user data (Parquet format)
│   ├── artists/                   # Processed artist data (Parquet format)
│   └── songplays/                 # Processed songplay data (Parquet format)
│

```


## ✅ Phase 3: Build & Run Spark ETL

### 🔥 ETL Overview

- Load raw `song_data` and `log_data` from S3
- Transform into dimensional model (`users`, `songs`, `artists`, `time`, `songplays`)
- Write back as Parquet into curated S3 paths

### ⚙️ Technologies Used

- [Apache Spark](https://spark.apache.org/) (PySpark)
- AWS S3 (`s3a://` access via `hadoop-aws`)
- Pandas (for local verification)
- JSON, Parquet

### 🚀 Run Spark Locally

```bash
spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.2.2 \
  spark_jobs/spark_etl.py

```

## 👨‍💻 Author
Chetara AbdelOuahab
Cloud Data Engineer in progress ☁️
Project repo: github.com/chetara/Sparkify_Cloud_Native-pipeline