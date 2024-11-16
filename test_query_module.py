# test_query_module.py
import pandas as pd
import pytest
from pyspark.sql import SparkSession
from query_module import create_queries_for_ruleitems_df_cp005

# Create a Spark session for testing
@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("Test").getOrCreate()

def test_create_queries_for_ruleitems_df_cp005(spark):
    # Sample input data simulating ruleitems_df_cp005 with relevant columns
    data = {
        "other_template_query": ["SELECT * FROM TABLE WHERE <JOIN_CONDITION> AND <RULE_EXPRESSION>"],
        "JOIN_CONDITION": ["col1 = col2"],
        "RULE_EXPRESSION": ["col3 > 100"],
        "LIST_OF_VALUES": [None],  # To check if None is handled correctly
        "RANGE_EXPRESSION": [None],
        "DIMENSION_KEY": [None],
        "BUSINESS_GROUP_LOCATION": ["location1"],
        "DATA_FILTER_EXPRESSION": [None],
        "DQ_COUNT_FILTER_EXPRESSION1": [None],
        "FILTER_OUT_CONDITION1": ["col4 != 'exclude_value'"]
    }
    
    # Convert the dictionary to a Pandas DataFrame
    ruleitems_df_cp005_pd = pd.DataFrame(data)
    
    # Convert Pandas DataFrame to Spark DataFrame
    ruleitems_df_cp005_spark = spark.createDataFrame(ruleitems_df_cp005_pd)

    # Call the function under test
    result_spark_df = create_queries_for_ruleitems_df_cp005(ruleitems_df_cp005_spark, spark)

    # Convert the result back to Pandas for easier assertion
    result_df = result_spark_df.toPandas()

    # Define expected output
    expected_other_template_query = "SELECT * FROM TABLE WHERE col1 = col2 AND col3 > 100 AND col4 != 'exclude_value' AND BUSINESS_GROUP_LOCATION = location1"

    # Check that the 'other_template_query' field was updated as expected
    assert result_df.loc[0, 'other_template_query'] == expected_other_template_query

def test_create_queries_for_ruleitems_df_cp005_no_replacements(spark):
    # Test case where there are no replacements needed
    data = {
        "other_template_query": ["SELECT * FROM TABLE"],
        "JOIN_CONDITION": [None],
        "RULE_EXPRESSION": [None],
        "LIST_OF_VALUES": [None],
        "RANGE_EXPRESSION": [None],
        "DIMENSION_KEY": [None],
        "BUSINESS_GROUP_LOCATION": [None],
        "DATA_FILTER_EXPRESSION": [None],
        "DQ_COUNT_FILTER_EXPRESSION1": [None],
        "FILTER_OUT_CONDITION1": [None]
    }
    
    # Convert the dictionary to a Pandas DataFrame
    ruleitems_df_cp005_pd = pd.DataFrame(data)
    
    # Convert Pandas DataFrame to Spark DataFrame
    ruleitems_df_cp005_spark = spark.createDataFrame(ruleitems_df_cp005_pd)

    # Call the function under test
    result_spark_df = create_queries_for_ruleitems_df_cp005(ruleitems_df_cp005_spark, spark)

    # Convert the result back to Pandas for easier assertion
    result_df = result_spark_df.toPandas()

    # Check that the 'other_template_query' field is unchanged
    assert result_df.loc[0, 'other_template_query'] == "SELECT * FROM TABLE"

def test_create_queries_for_ruleitems_df_cp005_static_replacements(spark):
    # Test case for static replacements like BUSINESS_DATE_FILTER
    data = {
        "other_template_query": ["SELECT * FROM TABLE WHERE <BUSINESS_DATE_FILTER>"],
        "JOIN_CONDITION": [None],
        "RULE_EXPRESSION": [None],
        "LIST_OF_VALUES": [None],
        "RANGE_EXPRESSION": [None],
        "DIMENSION_KEY": [None],
        "BUSINESS_GROUP_LOCATION": [None],
        "DATA_FILTER_EXPRESSION": [None],
        "DQ_COUNT_FILTER_EXPRESSION1": [None],
        "FILTER_OUT_CONDITION1": [None]
    }
    
    # Convert the dictionary to a Pandas DataFrame
    ruleitems_df_cp005_pd = pd.DataFrame(data)
    
    # Convert Pandas DataFrame to Spark DataFrame
    ruleitems_df_cp005_spark = spark.createDataFrame(ruleitems_df_cp005_pd)

    # Call the function under test
    result_spark_df = create_queries_for_ruleitems_df_cp005(ruleitems_df_cp005_spark, spark)

    # Convert the result back to Pandas for easier assertion
    result_df = result_spark_df.toPandas()

    # Expected output after replacing <BUSINESS_DATE_FILTER> with 'BUSINESS_DATE = :'
    expected_other_template_query = "SELECT * FROM TABLE WHERE BUSINESS_DATE = :"

    # Check that the 'other_template_query' field was updated as expected
    assert result_df.loc[0, 'other_template_query'] == expected_other_template_query

