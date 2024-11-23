import pytest
import pandas as pd
import os

# Directory containing the data files
DATA_DIR = "data"

def get_file_paths(region, date):
    """
    Generate file paths for input and expected CSVs based on region and date.
    """
    input_file = os.path.join(DATA_DIR, f"input_{region}_{date}.csv")
    expected_file = os.path.join(DATA_DIR, f"expected_{region}_{date}.csv")
    return input_file, expected_file

# Define test parameters for regions and dates
regions = ["NA", "EU", "APAC"]
dates = ["20231120", "20231121"]

# Generate all combinations of region and date
test_params = [(region, date) for region in regions for date in dates]

# Add descriptive test IDs
test_ids = [f"{region}_{date}" for region, date in test_params]

@pytest.fixture
def load_data(request):
    """
    Fixture to load input and expected data for a specific region and date.
    """
    region, date = request.param
    input_file, expected_file = get_file_paths(region, date)

    # Validate that files exist
    if not os.path.exists(input_file):
        pytest.fail(f"Input file missing: {input_file}")
    if not os.path.exists(expected_file):
        pytest.fail(f"Expected file missing: {expected_file}")

    # Load the data
    input_df = pd.read_csv(input_file)
    expected_df = pd.read_csv(expected_file)

    return input_df, expected_df

@pytest.mark.parametrize("region, date", test_params, ids=test_ids)
@pytest.mark.parametrize("load_data", test_params, indirect=True)
def test_case1(region, date, load_data):
    """
    Test case 1: Validate specific columns in the DataFrame.
    """
    input_df, expected_df = load_data

    # Example: Validate column 'A'
    pd.testing.assert_series_equal(input_df["A"], expected_df["A"])

@pytest.mark.parametrize("region, date", test_params, ids=test_ids)
@pytest.mark.parametrize("load_data", test_params, indirect=True)
def test_case2(region, date, load_data):
    """
    Test case 2: Validate another set of columns.
    """
    input_df, expected_df = load_data

    # Example: Validate column 'B'
    pd.testing.assert_series_equal(input_df["B"], expected_df["B"])

@pytest.mark.parametrize("region, date", test_params, ids=test_ids)
@pytest.mark.parametrize("load_data", test_params, indirect=True)
def test_case3(region, date, load_data):
    """
    Test case 3: Validate the entire DataFrame.
    """
    input_df, expected_df = load_data

    # Example: Validate the entire DataFrame
    pd.testing.assert_frame_equal(input_df, expected_df)