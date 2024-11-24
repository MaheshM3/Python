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
def test_case1(region, date, type_, load_data):
    """
    Test case 1: Validate specific columns for type='CP'.
    """
    if type_ != "CP":
        pytest.skip("Test case 1 is only applicable for type='CP'")
    
    input_df, expected_df = load_data

    # Validate columns 'Id' and 'A'
    pd.testing.assert_frame_equal(input_df[["Id", "A"]], expected_df[["Id", "A"]])

@pytest.mark.parametrize("region, date, type_", test_params, ids=test_ids)
@pytest.mark.parametrize("load_data", test_params, indirect=True)
def test_case2(region, date, type_, load_data):
    """
    Test case 2: Validate a different set of columns for type='FULL'.
    """
    if type_ != "FULL":
        pytest.skip("Test case 2 is only applicable for type='FULL'")
    
    input_df, expected_df = load_data

    # Validate columns 'B' and 'C'
    pd.testing.assert_frame_equal(input_df[["B", "C"]], expected_df[["B", "C"]])

@pytest.mark.parametrize("region, date, type_", test_params, ids=test_ids)
@pytest.mark.parametrize("load_data", test_params, indirect=True)
def test_case3(region, date, type_, load_data):
    """
    Test case 3: Validate the entire DataFrame for both type='CP' and type='FULL'.
    """
    if type_ not in ["CP", "FULL"]:
        pytest.skip("Test case 3 is only applicable for type='CP' or 'FULL'")
    
    input_df, expected_df = load_data

    # Validate the entire DataFrame
    pd.testing.assert_frame_equal(input_df, expected_df)
