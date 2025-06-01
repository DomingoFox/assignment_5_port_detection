
from pyspark.sql.functions import col, to_timestamp, round
from pyspark.sql.types import DoubleType
from helpers.logger import log
import glob
import os
import shutil

def setup_spark(log_level='ERROR'):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
    .appName("Vessel analysis") \
    .master("local[*]") \
    .config("spark.hadoop.hadoop.home.dir", "C:\\hadoop") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

    spark.sparkContext.setLogLevel(log_level)
    return spark

def read_file(spark, filepath, header=True, inferSchema=True):
    df = spark.read.csv(filepath, header=header, inferSchema=inferSchema)
    log.info(f"Initial record count: {df.count()}")
    return df

def clean_data(df):
    # Casting columns to appropriate types, filtering noise and removing duplicates.
    log.info("Casting columns and filtering noise...")

    df = df.withColumnRenamed("Navigational status", "navigational_status")
    df = df.withColumnRenamed("# Timestamp", "Timestamp")
    df = df.select(
        col("MMSI").cast("int"),
        to_timestamp(col("Timestamp"), "dd/MM/yyyy HH:mm:ss").alias("Timestamp"),
        col("Latitude").cast(DoubleType()),
        col("Longitude").cast(DoubleType()),
        col("SOG").cast(DoubleType()).alias("Speed"),
        col('Destination').cast('string'),
        col('navigational_status').cast('string')
    ).filter(
        col("MMSI").isNotNull() & # These 4 values cant be null, because they are needed for determining exact location and time of specific vessel.
        col("Timestamp").isNotNull() &
        col("Latitude").isNotNull() &
        col("Longitude").isNotNull() &
        (col("Latitude").between(-90, 90)) & # Invalid values that fall out of bound this bound
        (col("Longitude").between(-180, 180)) & # Invalids values that fall out of this bound 
        (col("Speed") < 0.5)  # Filter low-speed vessels (likely at ports)
    )

    df = df.repartition(200, "MMSI").cache()

    log.info(f"Keeping only relevant Navigational statuses (Moored, At anchor, Not under command).")
    at_port_statuses = [
        "Moored",
        "At anchor",
        "Not under command"
    ]
    df = df.filter(col("navigational_status").isin(at_port_statuses))
    
    log.info(f"Removing duplicates (Same Timestamp for same MMSI, similar coords for same MMSI).")
    # Removing duplicates for MMSI and time
    df = df.dropDuplicates(["MMSI", "Timestamp"])

    # We can reduce data for same MMSI, which contains the same Latitude and Longtitude coordinates when rounded to 0.01 (~1 km)
    df = df.withColumn("lat_rounded", round(col("Latitude"), 2)) \
       .withColumn("lon_rounded", round(col("Longitude"), 2))
    df = df.dropDuplicates(["MMSI", "lat_rounded", "lon_rounded"])
    # Removing helper columns
    df = df.drop("lat_rounded", "lon_rounded")
    return df


def write_csv(df, output_path, filename):
    log.info(f"Final dataset contains {df.count()} rows. Writing to output...")
    df.coalesce(1) \
            .write.mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
    
    spark_generated_name = glob.glob(os.path.join(output_path, "part-*.csv"))[0]
    shutil.move(spark_generated_name, os.path.join(output_path, filename))
    log.info("Write complete.")


def main(filepath, output_dir, output_path):
    spark = setup_spark()
    try:
        df = read_file(spark, filepath)
        df = clean_data(df)
        write_csv(df, output_dir, output_path)
    finally:
        spark.stop()

if __name__ == "__main__":
    filepath = 'files/aisdk-2025-03-01.csv'
    output_dir = 'files/cleaned/'
    output_path = 'cleaned-aisdk-2025-03-01.csv'
    main(filepath, output_dir, output_path)
