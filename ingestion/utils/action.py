# import os
# from pyspark.sql import SparkSession
# from dotenv import load_dotenv

# # Load environment variables from .env file
# load_dotenv()

# # Function to create a Spark session
# def create_spark_session():
#     return (
#         SparkSession.builder
#         .appName("Nessie Table Cleanup")
#         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
#         .config("spark.sql.catalog.nessie", "org.apache.iceberg.nessie.NessieCatalog")
#         .config("spark.sql.catalog.nessie.uri", os.getenv("NESSIE_API_URI", "http://localhost:19120/api/v1"))
#         .config("spark.sql.catalog.nessie.warehouse", os.getenv("NESSIE_WAREHOUSE", "s3a://lakehouse/"))
#         .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
#         .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
#         .config("spark.hadoop.fs.s3a.endpoint", os.getenv("AWS_S3_ENDPOINT", "http://localhost:9000"))
#         .config("spark.hadoop.fs.s3a.path.style.access", "true")
#         .config(
#             "spark.jars.packages",
#             ",".join([
#                 "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1",
#                 "org.projectnessie:nessie-spark-extensions-3.3_2.12:0.67.0",
#                 "software.amazon.awssdk:url-connection-client:2.17.178",
#             ])
#         )
#         .getOrCreate()
#     )

# # Main function
# def main():
#     try:
#         # Create Spark session
#         spark = create_spark_session()

#         # Drop the Iceberg table if it exists
#         table_name = "nessie.bronze.chargenet.stations"
#         print(f"Attempting to drop table: {table_name}")
#         spark.sql(f"DROP TABLE IF EXISTS {table_name}")
#         print(f"Table {table_name} dropped successfully (if it existed).")

#     except Exception as e:
#         print(f"Error occurred: {e}")

#     finally:
#         # Stop the Spark session
#         if 'spark' in locals():
#             spark.stop()
#             print("Spark session stopped.")

# # Entry point
# if __name__ == "__main__":
#     main()



# fs.s3a.endpoint--------> minio:9000
# fs.s3a.path.style.access--------> true
# dremio.s3.compat----------> true


# python -m ingestion.bronze.chargenet_ingest
# python -m ingestion.bronze.ecoride_ingest
# python -m ingestion.bronze.vehicle_health_ingest
# python -m ingestion.bronze.mssql_ingest
# python -m ingestion.bronze.pgsql_ingest
# python -m ingestion.bronze.mongo_ingest 
# python -m ingestion.bronze.documents_ingest
