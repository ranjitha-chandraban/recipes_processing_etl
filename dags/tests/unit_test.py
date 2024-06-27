import pytest
from pyspark.sql import SparkSession
from data_preparation.data_prep import pattern_matching_rows,to_convert_into_minutes,add_difficult_level

data_dict=dict()
# Fixture to create a SparkSession
@pytest.fixture(scope="session")

def spark_session():
    r"""
    This function creates a spark session

    Returns:
        return_type: spark app object"""
    return SparkSession.builder \
        .master("local") \
        .appName("Recipe_Processor_Test") \
        .getOrCreate()

# Test functions
def test_pattern_matching_rows(spark_session: SparkSession):
    """To test a function which  rows of  a particular column that matches the specified pattern """
    # Create a test DataFrame
    data = [("This is testing beef",),
        ("beef",),
        ("This is not befe",),
        ("this is bfffe",),
        ("It's fefe Powder",)]

# Define the schema for the DataFrame
    schema = ["Ingredients"]
    df = spark_session.createDataFrame(data, schema)

    # Call the function 
    result_df = pattern_matching_rows(df,"Ingredients","r'(?i)\bbeef\b'")

    # Check the result
    data = [("This is testing beef",),
        ("beef",)]
    schema = ["Ingredients"]
    expected_df = spark_session.createDataFrame(data, schema)
    print("EXPECTED",expected_df.collect())
    print("RESULT",result_df.collect())
    assert result_df.collect() == expected_df.collect()
    
def test_convert_into_minutes(spark_session: SparkSession):
    """To test on converting a column containing format "PT5H4M" data into  minutes with a new column name """
    data = [("PT5M",),
        ("PT1H5M",),
        ("PT15M",),
        ("PT35M",),
        ("PT3H5M",)]

# Define the schema for the DataFrame
    schema = ["prepTime"]
    df = spark_session.createDataFrame(data, schema)

    # Call the function
    result_df = to_convert_into_minutes(df,"prepTime")
    result_df.show(truncate=False)
    # Check the result
    data = [("PT5M", 5),
        ("PT1H5M",65),
        ("PT15M",15),
        ("PT35M",35),
        ("PT3H5M",185)]
    schema = ["prepTime","prepTimetotalminutes"]
    expected_df = spark_session.createDataFrame(data, schema)
    assert result_df.collect() == expected_df.collect()

def test_add_difficult_level(spark_session: SparkSession):
    """To test on adding a column named difficulty values as "HARD,MEDIUM,EASY" based on totalminutes column conditions"""
    data = [(45,),
        (68,),
        (35,),
        (29,),
        (30,)]

# Define the schema for the DataFrame
    schema = ["totalminutes"]
    df = spark_session.createDataFrame(data, schema)

    # Call the function
    result_df = add_difficult_level(df,"Difficulty","totalminutes",30,60)
    result_df.show(truncate=False)
    # Check the result
    data = [(45, "MEDIUM"),
        (68,"HARD"),
        (35,"MEDIUM"),
        (29,"EASY"),
        (30,"MEDIUM")]
    schema = ["Difficulty","totalminutes"]
    expected_df = spark_session.createDataFrame(data, schema)
    assert result_df.collect() == expected_df.collect()