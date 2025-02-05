'''
The purpose is to create and replace Iceberg tables

**** Queries from MMI
1. If there is same csv file but data is dirrefent what will happen and what is the solution in case of incremental data by csv
'''

from pyspark.sql import SparkSession
import logging

class DataIngestor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def create_or_replace_iceberg_table(self, df, layer, business_entity, table_name):
        # Generate SQL statement for table creation
        schema = df.schema
        layer=layer.lower()
        business_entity=business_entity.lower()
        table_name=table_name.lower()
        
        # Generate SQL statement for table creation        
        schema_sql = ', '.join([f"{field.name} {field.dataType.simpleString()}" for field in schema.fields])        
        create_table_sql = f"CREATE OR REPLACE TABLE nessie.{layer}.{business_entity}.{table_name} ({schema_sql}) USING iceberg"

        # Execute the SQL statement
        try:
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS nessie.{layer};")
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS nessie.{layer}.{business_entity.lower()};")
            self.spark.sql(create_table_sql)            
            logging.info(f"Iceberg table nessie.{layer}.{business_entity}.{table_name} created/replaced successfully.")
        except Exception as e:
            logging.error(f"Error creating/replacing Iceberg table: {e}")
            raise e

    # def create_or_replace_iceberg_table(self, df, layer, business_entity, table_name):
    #     """
    #     Creates or replaces an Iceberg table in Nessie.
        
    #     Args:
    #     - df: PySpark DataFrame (schema will be used to create table)
    #     - layer: Data layer (e.g., "bronze")
    #     - business_entity: The business domain (e.g., "sales")
    #     - table_name: The table name (e.g., "orders")
    #     """

    #     # Convert schema & table name to lowercase (Iceberg best practice)
    #     namespace = f"nessie.{layer}.{business_entity}" #.lower()
    #     formatted_table = f"{namespace}.{table_name}" #.lower()

    #     # Generate SQL schema definition from DataFrame schema
    #     schema_sql = ', '.join([f"{field.name} {field.dataType.simpleString()}" for field in df.schema.fields])
    #     create_table_sql = f"CREATE OR REPLACE TABLE {formatted_table} ({schema_sql}) USING iceberg"
        

    #     try:
    #         # Ensure namespace exists before creating the table
    #         self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    #         # Create or replace Iceberg table
    #         self.spark.sql(create_table_sql)

    #         logging.info(f"Iceberg table created/replaced successfully: {formatted_table}")
        
    #     except Exception as e:
    #         logging.error(f"Error creating/replacing Iceberg table: {e}")
    #         raise e    
        


    def ingest_file_to_bronze(self, file_path: str, business_entity: str, table_name: str, file_type: str, partition_by=None):
        try:
            if file_type == 'csv':
                df = self.spark.read.csv(file_path, header=True, inferSchema=True)
            elif file_type == 'json':
                df = self.spark.read.option("multiLine", "true").json(file_path)
            else:
                raise ValueError(f"Unsupported file type '{file_type}'. Supported types: csv, json")

            # Construct the full table path
            full_table_path = f"nessie.bronze.{business_entity}.{table_name}"

            # Ensure the Iceberg table exists
            self.create_or_replace_iceberg_table(df, "bronze", business_entity, table_name)

            # Write data to Iceberg table with partitioning if specified
            if partition_by:
                df.write.format("iceberg").partitionBy(partition_by).mode("overwrite").save(full_table_path)
                #df.write.format("iceberg").partitionBy("date_column").mode("append").save(full_table_path)
            else:
                df.write.format("iceberg").mode("overwrite").save(full_table_path)
                # in case of incremental data or if there is any possibility of incremental data the following commented code might work
                #df.write.format("iceberg").mode("append").save(full_table_path)
            
            logging.info(f"Data ingested successfully from {file_path} to Iceberg table {full_table_path}")
        
        except Exception as e:
            logging.error(f"Error in ingesting file to Iceberg table: {e}")
            raise e
        
    # @MMI @ 29 Jan 2025 
    def ingest_rdbms_to_bronze(self,business_entity,url,properties,db_schema,table_name,db_type):  

        if db_schema=="":
            logging.info(f"Schema Name is not defined.!")

        if db_type=="mssql":
            full_table_name = f'{db_schema}.{table_name}'
        elif db_type=="pgsql":
            full_table_name = f'"{db_schema}"."{table_name}"'

        try:
            # Construct the full table path
            full_table_path = f"nessie.bronze.{business_entity}.{table_name}" 

            #Get Data from Database as DataFrame
            df = self.spark.read.jdbc(url=url, table=full_table_name, properties=properties)

            # Ensure the Iceberg table exists
            self.create_or_replace_iceberg_table(df, "bronze", business_entity, table_name)            

            # Ensure Iceberg table exists
            df.write.format("iceberg").mode("overwrite").save(full_table_path)
            
            logging.info(f"Data ingested successfully from RDBMS to Iceberg table {full_table_path}")
        except Exception as e:
            # logging
            logging.error(f"Error in ingesting file to Iceberg table: {e}")
            raise e   

    # Added for mongo DB
    def ingest_mongo_to_bronze(self,uri,database,collection):
        try:        
            #Get Data from Database as DataFrame            
            df = self.spark.read.format("mongodb") \
                .option("uri", uri) \
                .option("database",database) \
                .option("collection",collection) \
                .load()
            
            ice_db=f"`{database}`" if '-' in database else database
            ice_collection=f"`{collection}`" if '-' in collection else collection

            # Construct the full table path
            full_table_path = f"nessie.bronze.{ice_db}.{ice_collection}"         
            
            # Ensure the Iceberg table exists
            self.create_or_replace_iceberg_table(df, "bronze", ice_db, ice_collection)  

            # Ensure Iceberg table exists
            df.write.format("iceberg").mode("overwrite").save(full_table_path) 

            logging.info(f"Data ingested successfully from MongoDB to Iceberg table {full_table_path}") 
        except Exception as e:
            # logging
            logging.error(f"Error in ingesting file to Iceberg table: {e}")
            raise e          
                      
        

            

        


