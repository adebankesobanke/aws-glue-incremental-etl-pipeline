from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count
from pyspark.sql.functions import max as spark_max
import boto3

# -----------------------
# CREATE SPARK SESSION
# -----------------------

spark = SparkSession.builder.appName("final-model").getOrCreate()

# -----------------------
# S3 + WATERMARK CONFIG
# -----------------------

s3 = boto3.client("s3")

bucket_name = "hospital-etl-data-adebanke"
watermark_key = "metadata/watermark.txt"

# -----------------------
# READ WATERMARK FROM S3
# -----------------------

response = s3.get_object(
    Bucket=bucket_name,
    Key=watermark_key
)

last_processed_date = response["Body"].read().decode("utf-8").strip()

print("Last processed date:", last_processed_date)

# -----------------------
# LOAD DATA
# -----------------------

appointments = spark.read.csv(
    "s3://hospital-etl-data-adebanke/raw/appointments/",
    header=True,
    inferSchema=True
)

patients = spark.read.csv(
    "s3://hospital-etl-data-adebanke/raw/patients/",
    header=True,
    inferSchema=True
)

doctors = spark.read.csv(
    "s3://hospital-etl-data-adebanke/raw/doctors/",
    header=True,
    inferSchema=True
)

branches = spark.read.csv(
    "s3://hospital-etl-data-adebanke/raw/branches/",
    header=True,
    inferSchema=True
)

billings = spark.read.csv(
    "s3://hospital-etl-data-adebanke/raw/billings/",
    header=True,
    inferSchema=True
)

# -----------------------
# FILTER ONLY NEW DATA
# -----------------------

appointments = appointments.filter(
    col("appointment_date") > last_processed_date
)

# -----------------------
# CLEAN DIMENSIONS
# -----------------------

patients_clean = patients.select(
    "patient_id",
    col("first_name").alias("patient_first_name"),
    col("last_name").alias("patient_last_name")
)

doctors_clean = doctors.select(
    "doctor_id",
    col("first_name").alias("doctor_first_name"),
    col("last_name").alias("doctor_last_name"),
    "specialization"
)

branches_clean = branches.select(
    "branch_id",
    "branch_name"
)

appointments_clean = appointments.select(
    "appointment_id",
    "patient_id",
    "doctor_id",
    "branch_id",
    "appointment_date",
    "status"
)

# -----------------------
# BUILD CORE MODEL
# -----------------------

base_df = appointments_clean \
    .join(patients_clean, "patient_id", "left") \
    .join(doctors_clean, "doctor_id", "left") \
    .join(branches_clean, "branch_id", "left")

# -----------------------
# FACT AGGREGATION
# -----------------------

billing_agg = billings.groupBy("patient_id").agg(
    sum("amount").alias("total_amount"),
    count("amount").alias("billing_count")
)

# -----------------------
# FINAL MODEL
# -----------------------

final_df = base_df.join(
    billing_agg,
    "patient_id",
    "left"
)

# -----------------------
# DATA QUALITY CHECKS
# -----------------------

df_clean = final_df.filter(
    col("patient_id").isNotNull() &
    col("doctor_id").isNotNull() &
    col("appointment_id").isNotNull()
)

# -----------------------
# FINAL SCHEMA
# -----------------------

df_clean = df_clean.select(
    "appointment_id",
    "patient_id",
    "doctor_id",
    "branch_id",
    "appointment_date",
    "status",
    "patient_first_name",
    "patient_last_name",
    "doctor_first_name",
    "doctor_last_name",
    "specialization",
    "branch_name",
    "total_amount",
    "billing_count"
)

# -----------------------
# REPARTITION FOR OPTIMIZED WRITES
# -----------------------

df_clean = df_clean.repartition("appointment_date")

# -----------------------
# ETL EXECUTION
# -----------------------

try:

    # -----------------------
    # WRITE PARQUET OUTPUT
    # -----------------------

    df_clean.write \
        .mode("append") \
        .partitionBy("appointment_date") \
        .parquet(
            "s3://hospital-etl-data-adebanke/curated/patient_revenue_summary/v3/"
        )

    print("Parquet write successful")

    # -----------------------
    # COMPUTE NEW WATERMARK
    # -----------------------

    new_watermark = df_clean.agg(
        spark_max("appointment_date")
    ).collect()[0][0]

    print("New watermark:", new_watermark)

    # -----------------------
    # UPDATE WATERMARK IN S3
    # -----------------------

    if new_watermark is not None:

        s3.put_object(
            Bucket=bucket_name,
            Key=watermark_key,
            Body=str(new_watermark)
        )

        print("Watermark updated successfully")

    else:

        print("No new records found. Watermark not updated.")

except Exception as e:

    print("ETL FAILED")
    print(str(e))

    raise e