import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException

# Import your Spark function
from your_module import your_function  # Replace with your actual module and function name


@pytest.fixture(scope="module")
def spark_session():
    """Fixture for creating a Spark session."""
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("pytest-pyspark") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_function(spark_session):
    # Read input and expected output CSVs into Spark DataFrames
    input_df = spark_session.read.csv("input.csv", header=True, inferSchema=True)
    expected_output_df = spark_session.read.csv("expected_output.csv", header=True, inferSchema=True)

    # Call the function with the input Spark DataFrame
    output_df = your_function(input_df)

    # Sort both DataFrames to ensure consistent comparison
    output_sorted = output_df.orderBy(*output_df.columns)
    expected_sorted = expected_output_df.orderBy(*expected_output_df.columns)

    # Collect data and compare as lists (to ensure content equality)
    assert output_sorted.collect() == expected_sorted.collect(), "DataFrames do not match!"