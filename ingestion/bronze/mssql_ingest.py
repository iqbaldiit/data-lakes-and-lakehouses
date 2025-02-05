from ingestion.utils.spark_utils import create_spark_session
from ingestion.utils.data_ingestor import DataIngestor
from ingestion.utils.config_loader import ConfigLoader

if __name__ == "__main__":
    config = ConfigLoader()
    spark = create_spark_session()
    ingestor = DataIngestor(spark)    
    
    # Ingest all employes to the bronz layer    
    ingestor.ingest_rdbms_to_bronze("hrm",config.mssql_uri,config.mssql_properties,"dbo","Employee","mssql")

    