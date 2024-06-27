from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, regexp_extract, length
from file_utils.utils import read_file
from pyspark.sql import SparkSession
import psutil
import os
import logging

# Create a logger instance
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def get_spark_session():
    r"""
    This method creates a spark session

    Returns:
        return_type: spark app object"""
    try:
        spark = SparkSession.builder \
            .appName('HelloFresh') \
            .master("local[1]") \
            .getOrCreate()
    except Exception as e:
        logger.info("Error in the data_validation:get_spark_session function")
        logger.exception("Error in get_data_validation:get_spark_session function " + str(e))
    return spark


def validate_non_null(df) -> bool:
    r"""
    This method verifies whether there are any null and Nan values in the given Dataframe

    Args:
        df (DataFrame): Dataframe

    Returns:
        return_type: Boolean"""
    try:
        non_null_columns = [
            "name",
            "ingredients",
            "cookTime",
            "prepTime",
            "datePublished"]
        for column in non_null_columns:
            if df.filter(col(column).isNull()).count() > 0:
                return False
        return True
    except Exception as e:
        logger.info("Error in excecuting the data_validation:validate_non_null function")
        logger.exception("Error in excecuting the data_validation:validate_non_null function " + str(e))


def validate_date_format(df) -> bool:
    r"""
    This method validates whether the dates are in the required format

    Args:
        df (DataFrame): Dataframe

    Returns:
        return_type: Boolean"""
    try:
        date_pattern = r'^\d{4}-\d{2}-\d{2}$'
        filtered_df = df.filter(col("datePublished").rlike(date_pattern))
        # Check if all rows match the pattern
        return filtered_df.count() == df.count()
    except Exception as e:
        logger.info("Error in excecuting the data_validation:validate_date_format function")
        logger.exception("Error in excecuting the data_validation:validate_date_format function " + str(e))


def data_validate(base_path: str, input_folder: str) -> bool:
    r"""
    This method encapsulates all the different validation methods

    Args:
        base_path (str) : Root directory
        input_folder (str) : Input directory containing the *.json objects

    Returns:
        return_type: Boolean"""
    try:
        spark = get_spark_session()
        data = read_file(base_path, "json", input_folder, 'recipes-000.json', spark)
        validate_non_null_output = validate_non_null(data)
        validate_date_format_output = validate_date_format(data)
        return all([validate_non_null_output, validate_date_format_output])
    except Exception as e:
        raise
        logger.info("Error in excecuting the data_validation:data_validate function")
        logger.exception("Error in excecuting the data_validation:data_validate function " + str(e))