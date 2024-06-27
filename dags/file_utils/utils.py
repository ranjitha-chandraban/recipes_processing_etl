import shutil
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import logging
import yaml
from easydict import EasyDict
import os
import errno
logger = logging.getLogger(__name__)


def load_yaml_config(config_path, **kwargs):
    r"""
    This function loads the config files required for both Task-1 and Task-2

    Args:
        *str (config_path) : Expects the directory of the respective config files stored in *.yaml format.
        **kwargs

    Returns:
        return_type: Dataframe"""
    try:
        with open(config_path, **kwargs) as f:
            config = EasyDict(yaml.safe_load(f))
        return config
    except Exception as e:
        logger.info("Error in the utils:load_yaml_config function")
        logger.exception("Error in the utils:load_yaml_config function " + str(e))


def read_file(base_path: str, format: str, input_file_path: str, filename: str, spark: SparkSession) -> DataFrame:
    r"""
    This function reads the given input file in either *.json or *.parquet formats and returns a Dataframe object.

    Args:
        *str (base_path) : Expects the root directory of the project.
        *str (format) : Expects the type of the input format, ["json", "parquet"].
        *str (filename) : Expects the name of the file to be read
        *int (min_range) : Expects the Maximun range for "HARD" level
        spark (session): Current spark session

    Returns:
        return_type: Dataframe"""
    try:
        # Read *.json files
        if format == "json":
            logger.info("Reading input json file from local path")
            path_json = os.path.join(base_path, input_file_path, filename)
            df = spark.read.json(path_json)
            logger.info("The json file is read into pyspark df")
            return df
        # Read *.parquet files
        elif format == "parquet":
            logger.info(
                f"Reading input parquet file from {input_file_path} path")
            path_parquet = os.path.join(base_path, input_file_path)
            df = spark.read.option(
                "header",
                "true").option(
                "recursiveFileLookup",
                "true").parquet(path_parquet)
            logger.info("The parquet file is read into pyspark df")
            return df

    except Exception as e:
        logger.info("Error in the utils:read_file function")
        logger.exception("Error in the utils:read_file function " + str(e))
    return df


def mkdir_p(path: str):
    r"""
    This function creates a directory.

    Args:
        *str (path) : Expects the directory as a string"""
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise
    except Exception as e:
        raise

def write_csv(filename: str,spark: SparkSession,df: DataFrame,output_folder: str):
    r"""

        This function is used for writing the transformed data into "*.csv" files inside the "data/Today's_date" folder.

        Args:
            *str (filename) : Expects the name of the "*.csv" which is written in the "data/Today's_date" folder.
            *session (spark) : Expects the spark session object which is instantiated in the main file.
            *DataFrame (df) : Expects a dataframe object after all the required transformations are applied.
            *str (output_folder) : Expects the directory in which the output file is written.

    """
    try:
        dir_path = output_folder
        print("PATH", dir_path)
        # Get the current timestamp
        timestamp = datetime.now()
        # Extract the date part as a string
        date_string = timestamp.strftime("%Y-%m-%d")
        dir_path = os.path.join(dir_path, date_string)
        final_path = os.path.join(dir_path, filename)
        # Temp folder to collect and write the ".csv" objects coalesced from
        # distributed spark dfs into a single partition
        temp_path = os.path.join(dir_path, 'temp')
        df = df.coalesce(1)
        if not os.path.isdir(temp_path):
            mkdir_p(temp_path)
            df.write.options(header='True', delimiter='|').mode(
                "overwrite").csv(temp_path)

        # copying/renaming the .csv file to the required directory,
        # shutil.move() takes care of existing file/directory and overwrites
        # with the latest file/directory
        subfiles = os.listdir(temp_path)
        for fname in subfiles:
            if fname.endswith('.csv'):
                name = fname
        shutil.move(os.path.join(temp_path, name),
                    final_path, copy_function='copy2')
        shutil.rmtree(temp_path)
    except Exception as e:
        logger.info("The file is not written into output folder")
        logger.exception("Error in write_csv method " + str(e))
