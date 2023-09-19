import configparser
from pyspark.sql import SparkSession

def create_spark_session():
    """
    Create a SparkSession.
    """
    spark = SparkSession.builder \
        .appName("FullDataLoad") \
        .getOrCreate()
    return spark

def load_properties(table_name):
    """
    Load properties from properties file for a specific table.
    """
    config = configparser.ConfigParser()
    config.read('properties.ini')
    return config[table_name]

def process_table(table_name):
    """
    Process data for a specific table.
    """
    properties = load_properties(table_name)

    # Create SparkSession
    spark = create_spark_session()

    # Load data from the specified table
    df = spark.read \
        .format("jdbc") \
        .option("url", properties['db_url']) \
        .option("dbtable", properties['db_table']) \
        .option("user", properties['db_user']) \
        .option("password", properties['db_password']) \
        .load()

    # Check if DataFrame is correct
    df.show()

    # Write DataFrame to local path as parquet in append mode
    df.write.mode('append').parquet(properties['local_path'])

def main():
    tables = ['customers', 'items', 'orders', 'order_details', 'salesperson', 'ship_to']
    for table_name in tables:
        process_table(table_name)

if __name__ == '__main__':
    main()
