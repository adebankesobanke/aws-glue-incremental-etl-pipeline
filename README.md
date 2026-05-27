# AWS Glue Incremental ETL Pipeline

## Architecture Diagram


![Architecture Diagram](images/architecture-diagram.JPG)


## Overview


![Architecture Diagram](images/workflow.JPG)


![Architecture Diagram](images/ETL-JobRunSuccess.JPG)


![Architecture Diagram](images/cloudwatch-diagram.JPG)

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
