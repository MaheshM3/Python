import os
import toml
import json
import configparser
from pathlib import Path
import pandas as pd

def extract_requirements_txt(filepath):
    with open(filepath, 'r') as f:
        return [line.strip() for line in f if line.strip() and not line.startswith('#')]

def extract_pyproject_toml(filepath):
    with open(filepath, 'r') as f:
        pyproject_data = toml.load(f)
    return pyproject_data.get('tool', {}).get('poetry', {}).get('dependencies', {})

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

def get_dependencies_from_repo(repo_name, repo_path):
    dependencies = []
    
    # Check for requirements.txt
    req_file = Path(repo_path) / 'requirements.txt'
    if req_file.exists():
        for dep in extract_requirements_txt(req_file):
            dependencies.append((repo_name, dep.split('==')[0], dep.split('==')[-1] if '==' in dep else ''))

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
            dependencies.append((repo_name, dep.split('==')[0], dep.split('==')[-1] if '==' in dep else ''))
    
    # Check for setup.cfg
    cfg_file = Path(repo_path) / 'setup.cfg'
    if cfg_file.exists():
        for dep in extract_setup_cfg(cfg_file):
            dependencies.append((repo_name, dep.split('==')[0], dep.split('==')[-1] if '==' in dep else ''))

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
