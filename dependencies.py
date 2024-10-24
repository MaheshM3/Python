import os
import toml
import json
import configparser
from pathlib import Path
import pandas as pd
import re

# Regex pattern to extract package name and version constraint
version_pattern = re.compile(r'([a-zA-Z0-9_\-]+)([<>=!~]*[0-9.]+)?')

def extract_requirements_txt(filepath):
    with open(filepath, 'r') as f:
        return [line.strip() for line in f if line.strip() and not line.startswith('#')]

def extract_pyproject_toml(filepath):
    with open(filepath, 'r') as f:
        pyproject_data = toml.load(f)
    
    # Attempt to find dependencies in various possible locations
    possible_keys = [
        'dependencies',                   # Non-standard direct dependencies list
        'tool.poetry.dependencies',        # Poetry standard
        'tool.pdm.dependencies',           # PDM standard
        'project.dependencies',            # PEP 621 standard under [project]
    ]
    
    for key in possible_keys:
        keys = key.split('.')
        data = pyproject_data
        for subkey in keys:
            if subkey in data:
                data = data[subkey]
            else:
                data = None
                break
        
        # If we found dependencies, process them
        if data:
            if isinstance(data, dict):  # Dictionary-style dependencies
                return data
            elif isinstance(data, list):  # List-style dependencies
                parsed_deps = {}
                for dep in data:
                    package, version = parse_dependency(dep)
                    parsed_deps[package] = version
                return parsed_deps

    return {}

def extract_setup_py(filepath):
    dependencies = []
    with open(filepath, 'r') as f:
        for line in f:
            if 'install_requires' in line:
                start = line.find('[')
                end = line.find(']')
                if start != -1 and end != -1:
                    deps = line[start+1:end].split(',')
                    dependencies.extend([dep.strip().strip("'\"") for dep in deps])
    return dependencies

def extract_setup_cfg(filepath):
    """Extract dependencies from setup.cfg."""
    config = configparser.ConfigParser()
    config.read(filepath)
    
    if 'options' in config and 'install_requires' in config['options']:
        return [dep.strip() for dep in config['options']['install_requires'].splitlines() if dep]
    
    return []

def extract_package_json(filepath):
    with open(filepath, 'r') as f:
        package_data = json.load(f)
    return package_data.get('dependencies', {})

def parse_dependency(dep):
    """Extract package name and version constraint."""
    match = version_pattern.match(dep)
    if match:
        package = match.group(1)
        version_constraint = match.group(2) or ''
        return package, version_constraint
    return dep, ''  # If no match, return the dep as it is (could be a special case)

def get_dependencies_from_repo(repo_name, repo_path):
    dependencies = []
    
    # Check for requirements.txt
    req_file = Path(repo_path) / 'requirements.txt'
    if req_file.exists():
        for dep in extract_requirements_txt(req_file):
            package, version = parse_dependency(dep)
            dependencies.append((repo_name, package, version))

    # Check for pyproject.toml
    toml_file = Path(repo_path) / 'pyproject.toml'
    if toml_file.exists():
        py_deps = extract_pyproject_toml(toml_file)
        for dep, version in py_deps.items():
            dependencies.append((repo_name, dep, version))
    
    # Check for setup.py
    setup_file = Path(repo_path) / 'setup.py'
    if setup_file.exists():
        for dep in extract_setup_py(setup_file):
            package, version = parse_dependency(dep)
            dependencies.append((repo_name, package, version))
    
    # Check for setup.cfg
    cfg_file = Path(repo_path) / 'setup.cfg'
    if cfg_file.exists():
        for dep in extract_setup_cfg(cfg_file):
            package, version = parse_dependency(dep)
            dependencies.append((repo_name, package, version))

    # Check for package.json (for Node.js projects)
    package_json_file = Path(repo_path) / 'package.json'
    if package_json_file.exists():
        node_deps = extract_package_json(package_json_file)
        for dep, version in node_deps.items():
            dependencies.append((repo_name, dep, version))
    
    return dependencies

def process_repositories(repos_base_path):
    all_dependencies = []
    
    for repo_name in os.listdir(repos_base_path):
        repo_path = os.path.join(repos_base_path, repo_name)
        if os.path.isdir(repo_path):
            dependencies = get_dependencies_from_repo(repo_name, repo_path)
            all_dependencies.extend(dependencies)
    
    return all_dependencies

def generate_dependency_table(repos_base_path):
    dependencies = process_repositories(repos_base_path)
    
    # Convert list of dependencies into a DataFrame for better readability
    df = pd.DataFrame(dependencies, columns=["Repository", "Dependency", "Version/Constraint"])
    
    # Ensure that all columns are treated as strings before dropping duplicates
    df = df.astype(str)

    # Drop duplicates if needed
    df = df.drop_duplicates()

    # Print the table format (or save it to CSV/Excel)
    print(df.to_string(index=False))

    # Optionally, save to CSV or Excel
    df.to_csv('dependency_list.csv', index=False)
    df.to_excel('dependency_list.xlsx', index=False)

# Example usage:
if __name__ == "__main__":
    base_path = "/path/to/your/local/repositories"  # Update this to your repos' location
    generate_dependency_table(base_path)
