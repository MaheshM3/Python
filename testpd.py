import pandas as pd
import pytest

# The function you want to test
from your_module import your_function  # Replace with your actual module and function name

def test_function():
    # Load input and expected output dataframes from CSV
    input_df = pd.read_csv("input.csv")
    expected_output_df = pd.read_csv("expected_output.csv")

    # Call the function with the input dataframe
    output_df = your_function(input_df)

    # Use pandas testing function to check if the dataframes are equal
    pd.testing.assert_frame_equal(output_df, expected_output_df)