# session.py
from pyspark.sql import SparkSession
import os

spark_master = os.getenv("SPARK_MASTER", "local[*]")
hdfs_base_uri = os.getenv("HDFS_BASE_URI", "hdfs://namenode:8020")

spark = (
    SparkSession.builder
        .appName("fhir_hudi_lakehouse")
        .master(spark_master)
        .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        # IMPORTANT: default all FS ops to HDFS so we don’t see “Wrong FS: expected file:///”
        .config("spark.hadoop.fs.defaultFS", hdfs_base_uri)
        # Keeps Spark JSON reader tolerant of slightly odd timestamps from FHIR servers
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
)
