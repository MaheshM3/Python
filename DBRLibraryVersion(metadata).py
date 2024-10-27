import importlib.metadata
import pandas as pd
import os
import sys

# Detect DBR version from environment variables
dbr_version = os.getenv("DATABRICKS_RUNTIME_VERSION", "unknown_version")
python_version = sys.version.split()[0]  # Get only the version number (e.g., "3.8.10")

libraries_to_check = [
    "pandas", "numpy", "scipy", "scikit-learn", "matplotlib",
    "urllib3", "psutil", "pyspark"
]

# Function to get versions using importlib.metadata
def get_library_versions(libraries):
    data = []
    for lib in libraries:
        try:
            version = importlib.metadata.version(lib)
        except importlib.metadata.PackageNotFoundError:
            version = "Not installed"
        # Add Python version and DBR version to each entry
        data.append({
            "Library": lib, 
            "Version": version, 
            "DBR_Version": dbr_version,
            "Python_Version": python_version
        })
    return data

# Generate DataFrame with versions
library_versions_data = get_library_versions(libraries_to_check)
library_versions_df = pd.DataFrame(library_versions_data)

# Display DataFrame
display(library_versions_df)
