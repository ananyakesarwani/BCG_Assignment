import yaml

def load_csv_data_to_df(spark,
                        file_path,
                        inferSchema: str = "true",
                        header: bool = True):
    """
    Read CSV data
    :param spark: spark instance
    :param file_path: path to the csv file
    :return: dataframe
    """
    return spark.read.option("inferSchema", inferSchema).csv(file_path, header=header)


def read_yaml(file_path: str):
    """
    Read Config file in YAML format
    :param file_path: file path to config.yaml
    :return: dictionary with config details
    """
    with open(file_path, "r") as f:
        return yaml.safe_load(f)


def write_output(data, file_num: int):
    """
    Write data frame to csv
    :param data: int/string/pyspark dataframe
    :param file_num: output analysis result number
    :param sparkInstance: spark method
    :return: None
    """

    # dataFrame = data
    config_file_path = "config.yaml"
    # col_name = read_yaml(config_file_path).get("HEADER_NAMES").get(file_num)
    output_path = read_yaml(config_file_path).get("OUTPUT_PATH").get(file_num)

    # if isinstance(data, int) or isinstance(data, str):
    #     dataFrame = sparkInsance.createDataFrame([(data,)], [col_name])
    data.write.option("header", True).csv(output_path)
