from pyspark.sql import SparkSession

from utilities.analyzer import USVehicleAccidentAnalysis

if __name__ == '__main__':
    # Initialize sparks session
    spark = SparkSession \
        .builder \
        .appName("USVehicleAccidentAnalysis") \
        .getOrCreate()

    config_file_path = "config.yaml"
    spark.sparkContext.setLogLevel("ERROR")

    usvaa = USVehicleAccidentAnalysis(config_file_path=config_file_path)
    usvaa.configure(spark)
    usvaa.retreive_data(spark)
    usvaa()

    spark.stop()
