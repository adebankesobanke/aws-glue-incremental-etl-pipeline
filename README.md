# AWS Glue Incremental ETL Pipeline

## Architecture Diagram


![Architecture Diagram](images/architecture-diagram.JPG)


## Overview
## ETL Script
The PySpark ETL implementation is located here:

`scripts/hospital_etl_incremental.py`

The script performs:
- incremental watermark filtering
- Spark joins and aggregations
- parquet output generation
- automated watermark updates
- CloudWatch operational logging


Workflow screenshot
![Architecture Diagram](images/workflow.JPG)


ETL execution screenshot
![Architecture Diagram](images/ETL-JobRunSuccess.JPG)


CloudWatch screenshot
![Architecture Diagram](images/cloudwatch-logs.JPG)


This project implements a fully automated incremental ETL pipeline using AWS Glue, PySpark, Amazon S3, and CloudWatch.

The pipeline:
- processes hospital appointment data
- performs incremental filtering using watermarking
- stores partitioned parquet outputs
- uses workflow orchestration and conditional triggers# aws-glue-incremental-etl-pipeline

  ## Tech Stack

- AWS Glue
- PySpark
- Amazon S3
- CloudWatch

  ## Features

- Incremental watermark processing
- Workflow orchestration
- Partitioned parquet output
- Conditional triggers
