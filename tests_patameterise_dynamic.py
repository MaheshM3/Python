import pytest
import pandas as pd
import os

# Directory containing the data files
DATA_DIR = "data"

def get_file_paths(region, date, type_):
    """
    Generate file paths for input and expected CSVs based on region, date, and type.
    """
    input_file = os.path.join(DATA_DIR, f"input_{region}_{date}_{type_}.csv")
    expected_file = os.path.join(DATA_DIR, f"expected_{region}_{date}_{type_}.csv")
    return input_file, expected_file

# Define test parameters for regions, dates, and types
regions = ["NA", "EU", "APAC"]
dates = ["20231120", "20231121"]
types = ["CP", "FULL"]

# Generate all combinations of region, date, and type
test_params = [(region, date, type_) for region in regions for date in dates for type_ in types]

# Add descriptive test IDs
test_ids = [f"{region}_{date}_{type_}" for region, date, type_ in test_params]

@pytest.fixture
def load_data(request):
    """
    Fixture to load input and expected data for a specific region, date, and type.
    """
    region, date, type_ = request.param
    input_file, expected_file = get_file_paths(region, date, type_)

    # Validate that files exist
    if not os.path.exists(input_file):
        pytest.fail(f"Input file missing: {input_file}")
    if not os.path.exists(expected_file):
        pytest.fail(f"Expected file missing: {expected_file}")

    # Load the data
    input_df = pd.read_csv(input_file)
    expected_df = pd.read_csv(expected_file)

    return input_df, expected_df

@pytest.mark.parametrize("region, date, type_", test_params, ids=test_ids)
@pytest.mark.parametrize("load_data", test_params, indirect=True)
def test_dynamic_frame_comparison(region, date, type_, load_data):
    """
    Dynamically validate specific columns or the entire DataFrame based on the type.
    """
    input_df, expected_df = load_data

    # Define the columns to validate based on the type
    if type_ == "CP":
        columns_to_compare = ["Id", "A"]
    elif type_ == "FULL":
        columns_to_compare = input_df.columns  # Validate all columns
    else:
        pytest.skip(f"Unknown type: {type_}")

    # Subset the DataFrames to the relevant columns
    input_subset = input_df[columns_to_compare]
    expected_subset = expected_df[columns_to_compare]

    # Validate the selected columns
    pd.testing.assert_frame_equal(input_subset, expected_subset)
