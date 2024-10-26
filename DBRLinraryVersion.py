import pkg_resources
import pandas as pd
import json
import os

# Detect DBR version from environment variables
dbr_version = os.getenv("DATABRICKS_RUNTIME_VERSION", "unknown_version")

# Path to your Excel file with the list of libraries (ensure the path is correct for your environment)
excel_path = "/dbfs/FileStore/path_to_your_excel_file.xlsx"

# Load the library list from Excel
# Assume the libraries are listed under a column named "Library"
library_df = pd.read_excel(excel_path, engine="openpyxl")
libraries_to_check = library_df["Library"].tolist()

# Function to get versions using pkg_resources
def get_library_versions(libraries):
    versions = {lib: "Not installed" for lib in libraries}
    for dist in pkg_resources.working_set:
        if dist.project_name.lower() in libraries:
            versions[dist.project_name.lower()] = dist.version
    return versions

# Get the versions for the specified libraries
library_versions = get_library_versions([lib.lower() for lib in libraries_to_check])

# Display library versions
print("Library Versions:", library_versions)
print(f"Detected Databricks Runtime Version: {dbr_version}")

# Save results to JSON for easy comparison between DBR versions
output_path = f"/dbfs/FileStore/Library_Versions_DBR_{dbr_version}.json"
with open(output_path, "w") as f:
    json.dump(library_versions, f)

print(f"Library versions saved to: {output_path}")