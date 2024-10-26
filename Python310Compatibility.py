import requests
import sys

def get_pypi_metadata(package_name):
    """Fetch the package metadata from PyPI."""
    url = f"https://pypi.org/pypi/{package_name}/json"
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"⚠️ Package '{package_name}' not found on PyPI.")
        return None

def check_python_version_compatibility(package_name, target_version="3.10"):
    """Check if the package supports the target Python version."""
    metadata = get_pypi_metadata(package_name)
    
    if metadata is None:
        return package_name, "Not Found", False
    
    requires_python = metadata['info'].get('requires_python')
    
    if requires_python:
        print(f"Package '{package_name}' requires Python versions: {requires_python}")
        
        # Update logic to assume compatibility if version requirement is less than the target version
        if "3." in requires_python and int(requires_python.split(".")[1][0]) <= int(target_version.split(".")[1]):
            return package_name, requires_python, True
        elif ">=3.8" in requires_python:
            return package_name, requires_python, True
        else:
            return package_name, requires_python, False
    else:
        return package_name, "No requirement specified", True  # Assume True if no specific requirement

def check_multiple_libraries(libraries, target_version="3.10"):
    """Check a list of libraries for Python 3.10 compatibility."""
    results = []
    for lib in libraries:
        package, version_requirement, is_compatible = check_python_version_compatibility(lib, target_version)
        results.append((package, version_requirement, "Compatible" if is_compatible else "Not Compatible"))
    
    return results

def save_results_to_csv(results, filename="python_compatibility_results.csv"):
    """Save the results to a CSV file."""
    import csv
    
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Library", "Python Version Requirement", "Python 3.10 Compatibility"])
        writer.writerows(results)
    
    print(f"Results saved to {filename}")

if __name__ == "__main__":
    # Example: A list of libraries to check
    libraries_to_check = [
        "requests",
        "psutil",
        "numpy",
        "pandas",
        "nonexistent-package",  # Test for nonexistent package
        # Add more libraries as needed
    ]

    # Check compatibility for all libraries
    results = check_multiple_libraries(libraries_to_check, target_version="3.10")
    
    # Print the results
    for result in results:
        print(f"Library: {result[0]}, Requires: {result[1]}, Python 3.10 Compatibility: {result[2]}")
    
    # Optionally save the results to a CSV file
    save_results_to_csv(results)
