from ingestion.utils.spark_utils import create_spark_session
from ingestion.utils.data_ingestor import DataIngestor
from ingestion.utils.config_loader import ConfigLoader


if __name__ == "__main__":
    config = ConfigLoader()
    spark = create_spark_session()
    ingestor = DataIngestor(spark)        

    # Ingest air-quality to bronz layer    
    ingestor.ingest_mongo_to_bronze(config.mongo_uri,"air-quality","nairobi")

