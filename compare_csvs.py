import pandas as pd

def compare_csvs(file1: str, file2: str, check_exact: bool = True):
    """
    Compares two CSV files.
    
    If `check_exact` is True, ensures exact matches in structure and values, including column order.
    If `check_exact` is False, ignores column order but checks for content match.
    
    Args:
    - file1 (str): Path to the first CSV file.
    - file2 (str): Path to the second CSV file.
    - check_exact (bool): If True, checks for exact match in structure and values.
                          If False, only checks content match regardless of column order.
    
    Returns:
    - bool: True if the CSVs match based on `check_exact`, False otherwise.
    """
    try:
        # Load CSV files into dataframes
        df1 = pd.read_csv(file1)
        df2 = pd.read_csv(file2)

        if check_exact:
            # 1. Check for exact column match (names and order)
            if list(df1.columns) != list(df2.columns):
                print("The CSV files have different columns or column order.")
                print("Columns in file1:", list(df1.columns))
                print("Columns in file2:", list(df2.columns))
                return False
            
            # 2. Check for exact row count match
            if len(df1) != len(df2):
                print("The CSV files have a different number of rows.")
                print("Rows in file1:", len(df1))
                print("Rows in file2:", len(df2))
                return False
            
            # 3. Check for exact match in content (all cell values must match)
            comparison_df = df1.compare(df2)
            if not comparison_df.empty:
                print("Differences found in cell values between CSV files:")
                print(comparison_df)
                return False

            # If all checks pass, the files match exactly
            print("The CSV files match exactly in structure and values.")
            return True

        else:
            # Non-exact comparison: Ignore column order, focus on values
            # Sort columns for both dataframes to ensure consistent order
            df1_sorted = df1.sort_index(axis=1)
            df2_sorted = df2.sort_index(axis=1)

            # Check column names (ignoring order) and row counts
            if set(df1.columns) != set(df2.columns):
                print("The CSV files have different columns (order ignored).")
                print("Columns in file1:", set(df1.columns))
                print("Columns in file2:", set(df2.columns))
                return False
            
            if len(df1) != len(df2):
                print("The CSV files have a different number of rows.")
                print("Rows in file1:", len(df1))
                print("Rows in file2:", len(df2))
                return False

            # Compare the sorted dataframes for content match
            if not df1_sorted.equals(df2_sorted):
                print("Differences found in content between CSV files.")
                # Find specific differences
                diff_df = pd.concat([df1_sorted, df2_sorted]).drop_duplicates(keep=False)
                print(diff_df)
                return False
            
            # If no differences were found
            print("The CSV files match in content (column order ignored).")
            return True

    except Exception as e:
        print(f"An error occurred while comparing the CSV files: {e}")
        return False

# Example usage
file1 = "path/to/first.csv"
file2 = "path/to/second.csv"
compare_csvs(file1, file2, check_exact=True)
