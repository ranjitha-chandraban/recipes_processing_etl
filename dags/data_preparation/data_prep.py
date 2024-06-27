import pyspark
from pyspark.sql.functions import *
import logging
from file_utils.utils import read_file, write_csv
import psutil
from pyspark.sql import SparkSession
from pyspark import SparkConf
import os

# Create a logger instance
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def get_spark_session():
    r"""
    This function creates a spark session

    Returns:
        return_type: spark app object"""
    try:
        total_memory, total_cores = get_system_memory_and_cores()
        # Allocate memory for driver and executers respectively
        driver_memory = int(0.1 * total_memory)  # 10% of total memory
        # Memory per executor
        executor_memory = int(0.2 * total_memory / total_cores)
        executor_cores = int(0.2 * total_cores)  # Cores per executor
        # num_executors = total_cores
        num_executors = total_cores // executor_cores
        # The percentage of memory per spark session can be given as a variable to the spark config file
        conf = (
            SparkConf()
            .setAppName("HelloFresh")
            .setMaster("local[1]")
            .set("spark.sql.files.maxPartitionBytes", "134217728")
        )  # set as deafult value 128MB, can be used for file partitioning and performance tuning for large files
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
    except Exception as e:
        logger.info("Error in the data_prep:get_spark_session function")
        logger.exception("Error in the data_prep:get_spark_session function " + str(e))
    return spark


def get_system_memory_and_cores():
    r"""
    This function returns total memory and number of cores

    Returns:
        return_type: float and int"""
    try:
        total_memory = psutil.virtual_memory().total // (1024**3)  # in GB
        total_cores = psutil.cpu_count()
        return total_memory, total_cores
    except Exception as e:
        logger.info("Error in the data_prep:get_system_memory_and_cores function")
        logger.exception("Error in the data_prep:get_system_memory_and_cores function " + str(e))
    


def pattern_matching_rows(df: DataFrame, column_name: str, pattern: expr) -> DataFrame:
    r"""
    This function for filtering the rows of a given PySpark dataframe by matching any given pattern('beef' in our case) for a particular column.

    Args:
        df (DataFrame): Dataframe
        column_name (string): column name
        pattern (regex): pattern to match

    Returns:
        return_type: Dataframe"""
    try:
        expression = eval(pattern)
        # Filters the rows where the value matches the expression
        df = df.filter(df[column_name].rlike(rf"{expression}"))
        return df
    except Exception as e:
        logger.info("Error in the data_prep:pattern_matching_rows function")
        logger.exception("Error in the data_prep:pattern_matching_rows function " + str(e))


def to_convert_into_minutes(df: DataFrame, column_name: str) -> DataFrame:
    r"""
    This function converts the given column format"PT4H5M" into minutes

    Args:
        df (DataFrame): Dataframe
        column_name (string): column name

    Returns:
        return_type: Dataframe"""
    try:
        # Using Regular Expression to extract the times in minutes for computing the total(prep + cook) time in minutes
        df = (
            df.withColumn(
                "hours", regexp_extract(
                    col(column_name), "PT(\\d+)H", 1).cast("int")
            )
            .withColumn(
                "minutes", regexp_extract(
                    col(column_name), "H(\\d+)M", 1).cast("int")
            )
            .withColumn(
                "minutes2", regexp_extract(
                    col(column_name), "PT(\\d+)M", 1).cast("int")
            )
        )
        # Adds a new column to store the total(prep + cook) time in minutes and
        # drops the column which are not required any further
        df = df.na.fill({"hours": 0, "minutes": 0, "minutes2": 0})
        df = df.withColumn(
            f"{column_name}totalminutes",
            col("hours") * 60 + col("minutes") + col("minutes2"),
        )
        df = df.drop("hours", "minutes", "minutes2")
        return df
    except Exception as e:
        logger.info("Error in the data_prep:to_convert_into_minutes function")
        logger.exception("Error in to_convert_into_minutes function " + str(e))


def add_difficult_level(df: DataFrame, new_column: str, column_name: str, min_range: int, max_range: int) -> DataFrame:
    r"""
    This function adds a new column name "new_col" based on conditions  of the given column_name

    Args:
        df (DataFrame): Dataframe
        *str (new_column) : Expects the name of the new column("totalminutes" in our case) which captures the difficulty levels.
        *str (column_name) : Expects the name of the column("Difficulty" in our case) which is used for assigning the difficulty level.
        *int (min_range) : Expects the minimum range for "EASY" level
        *int (min_range) : Expects the Maximun range for "HARD" level

    Returns:
        return_type: Dataframe"""
    try:
        # Assigns difficulty level w.r.t total(prep + cook) time in minutes and stores it in a new column
        df = df.withColumn(
            new_column,
            when(col(column_name) > max_range, "HARD")
            .when(
                ((col(column_name) >= min_range) &
                 (col(column_name) <= max_range)),
                "MEDIUM",
            )
            .when(col(column_name) < min_range, "EASY")
            .otherwise("UNKNOWN"),
        )
        return df
    except Exception as e:
        logger.info("Error in the data_prep:add_difficult_level function")
        logger.exception("Error in the data_prep:add_difficult_level function " + str(e))


def agg(df: DataFrame, new_column: str, column_name: str) -> DataFrame:
    r"""
    This function adds a new column name "new_column" and aggregated avg for column_name specified as parameters

    Args:
        df (DataFrame): Dataframe
        *str (new_column) : Expects the name of the new column("Difficulty" in our case) required for grouping.
        *str (column_name) : Expects the name of the column("totalminutes" in our case) required for aggregating and averaging.

    Returns:
        return_type: Dataframe"""
    try:
        # Computes avg of total(prep + cook) time in minutes for each difficulty levels by grouping and aggregation
        grouped_avg = df.groupby(new_column).avg(column_name)
        grouped_avg = grouped_avg.select(new_column, "avg(totalminutes)")
        return grouped_avg
    except Exception as e:
        logger.info("Error in the data_prep:agg function")
        logger.exception("Error in the data_prep:agg function " + str(e))


def total_time(df: DataFrame, operand_cols: List, output_col: str) -> DataFrame:
    r"""
    This function computes the total(prep + cook) time in minutes

    Args:
        df (DataFrame): Dataframe
        *List (operand_cols) : Expects the name of the columns for prep and cook time.
        *str (output_col) : Expects the name to be assigned to new column capturing total_time.

    Returns:
        return_type: Dataframe"""
    try:
        logger.debug(f"All columns in df:{str(df.columns)}")
        logger.debug(f"Columns to apply action:{str(operand_cols)}")
        # Asserts whether both the prep and cook columns are available, then computes the total(prep + cook) time in minutes
        assert len(
            operand_cols) == 2, "There must be only 2 columns for this operation"
        df = df.withColumn(
            output_col, df[operand_cols[0]] + df[operand_cols[1]])
        return df
    except Exception as e:
        logger.info("Error in the data_prep:total_time function")
        logger.exception("Error in the data_prep:total_time function " + str(e))

# Dictionary comprising of names of all the functions as found in the "data_pre_process_config.yaml" 
# and "data_process_config.yaml" config files required for data processing
function_dict = {
    "pattern_matching_rows": pattern_matching_rows,
    "to_convert_into_minutes": to_convert_into_minutes,
    "add_difficult_level": add_difficult_level,
    "total_time": total_time,
    "agg": agg,
}

def pre_process_data(base_path: str, input_file_path: str, config):
    r"""
    This function performs the Task-1, i.e, loads the *.json objects, performs all the pre-processing operation using spark 
    and saves the intermediate Dataframe as parquet which is optimized for Task-2

    Args:
        df (DataFrame): Dataframe
        *str (input_file_path) : Expects the file path containing *.json objects.
        config : Expects the "data_prep_process" config object"""

    try:
        # Instantiate the spark session
        spark = get_spark_session()
        serial_ops = config.data_prep_process.serial_ops
        # Loops over all the set of input/output
        for serial_op in serial_ops:
            # Reads the *.json file as a spark Dataframe object
            data = read_file(
                base_path, "json", input_file_path, serial_op.input.name, spark
            )
            ops = serial_op.operations
            # Loops over all the data pre processing operations required for Task-1
            for op in ops:
                logger.info(f"Performing the operation: {op.operation}")
                if "kwargs" in op.keys():
                    data = function_dict[op.operation](
                        data, **op.kwargs)
        # Writes the intermediate Dataframe in parquet format partitioned by the column "datePublished"
        file_name = serial_ops[0].input.name.split(".")[0]
        intermediate_dir = os.path.join(base_path, "intermediate_parquet", file_name + ".parquet")
        data.write.parquet(
            intermediate_dir,
            partitionBy="datePublished",
        )
        spark.stop()
        logger.info("Please find the intermediate parquet files in the intermediate_parquet folder")
    except Exception as e:
        raise
        # logger.info("Error in prepare_data function")
        # logger.exception("Error in prepare_data function " + str(e))


def process_data(base_path: str, intermediate_folder: str, config):
    r"""
    This function performs the Task-2, i.e, loads the *.parquet objects, performs all the processing operation using spark 
    and saves *.csv files in the capturing avg total_time for different levels

    Args:
        df (DataFrame): Dataframe
        *str (intermediate_folder) : Expects the file path containing *.parquet objects.
        config : Expects the "data_prep_process" config object"""
    try:
        spark = get_spark_session()
        serial_ops = config.data_process.serial_ops
        # Loops over all the set of input/output
        for serial_op in serial_ops:
            # Reads the *.parquet file as a spark Dataframe object
            data = read_file(
                base_path,
                "parquet",
                intermediate_folder,
                serial_op.input.name,
                spark)
            ops = serial_op.operations
            # Loops over all the data processing operations required for Task-2
            for op in ops:
                logger.info(f"Performing the operation: {op.operation}")
                if "kwargs" in op.keys():
                    data = function_dict[op.operation](
                        data, **op.kwargs)
        file_name = serial_ops[0].input.name.split(".")[0] + "_result" + ".csv"
        output_dir = os.path.join(base_path, "output")
        # Writes the results of Task-2 as a *.csv file in the output directory 
        write_csv(file_name, spark, data, output_dir)
        spark.stop()
        logger.info("Please find the files in the output_folder")
    except Exception as e:
        raise
        # logger.info("Error in prepare_data function")
        # logger.exception("Error in prepare_data function " + str(e))
