import pkg_resources
import pandas as pd
import json
import os

# Detect DBR version from environment variables
dbr_version = os.getenv("DATABRICKS_RUNTIME_VERSION", "unknown_version")

# Path to your Excel file with the list of libraries (ensure the path is correct for your environment)
# excel_path = "/dbfs/FileStore/path_to_your_excel_file.xlsx"

# # Load the library list from Excel
# # Assume the libraries are listed under a column named "Library"
# library_df = pd.read_excel(excel_path, engine="openpyxl")
# libraries_to_check = library_df["Library"].tolist()

# List of libraries to check versions for
libraries_to_check = [
    "pandas",
    "numpy",
    "scipy",
    "scikit-learn",
    "matplotlib",
    "seaborn",
    "tensorflow",
    "pyspark"
]

# Function to get versions using pkg_resources
def get_library_versions(libraries):
    # Create a list of dictionaries with library names, versions, and DBR version
    data = []
    for lib in libraries:
        version = "Not installed"
        for dist in pkg_resources.working_set:
            if dist.project_name.lower() == lib.lower():
                version = dist.version
                break
        data.append({"Library": lib, "Version": version, "DBR_Version": dbr_version})
    return data

# Get the versions for the specified libraries
library_versions_data = get_library_versions(libraries_to_check)

# Convert the data to a DataFrame
library_versions_df = pd.DataFrame(library_versions_data)

# Display library versions as DataFrame
display(library_versions_df)

# # Optional: Save results to JSON and CSV for record-keeping
# output_json_path = f"/dbfs/FileStore/Library_Versions_DBR_{dbr_version}.json"
# output_csv_path = f"/dbfs/FileStore/Library_Versions_DBR_{dbr_version}.csv"

# # Save as JSON
# library_versions_df.to_json(output_json_path, orient="records", lines=True)
# print(f"Library versions saved to JSON: {output_json_path}")

# # Save as CSV
# library_versions_df.to_csv(output_csv_path, index=False)
# print(f"Library versions saved to CSV: {output_csv_path}")
