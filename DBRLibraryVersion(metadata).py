import importlib.metadata
import pandas as pd
import os

dbr_version = os.getenv("DATABRICKS_RUNTIME_VERSION", "unknown_version")
libraries_to_check = [
    "pandas", "numpy", "scipy", "scikit-learn", "matplotlib",
    "seaborn", "tensorflow", "pyspark"
]

# Function to get versions using importlib.metadata
def get_library_versions(libraries):
    data = []
    for lib in libraries:
        try:
            version = importlib.metadata.version(lib)
        except importlib.metadata.PackageNotFoundError:
            version = "Not installed"
        data.append({"Library": lib, "Version": version, "DBR_Version": dbr_version})
    return data

library_versions_data = get_library_versions(libraries_to_check)
library_versions_df = pd.DataFrame(library_versions_data)
display(library_versions_df)
