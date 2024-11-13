#!/bin/bash

# List of repository clone URLs
repos=(
    "https://github.com/user/repo1.git"
    "https://github.com/user/repo2.git"
    "https://github.com/user/repo3.git"
)

# Base directory where repositories will be stored
base_dir="path/to/your/repos"

# Create the base directory if it doesn't exist
mkdir -p "$base_dir"
cd "$base_dir" || exit 1

# Iterate over each repository URL
for repo_url in "${repos[@]}"; do
    # Extract the repo name from the URL (e.g., repo1 from https://github.com/user/repo1.git)
    repo_name=$(basename "$repo_url" .git)

    # Check if the repository directory already exists
    if [ -d "$repo_name" ]; then
        echo "Pulling latest changes in $repo_name..."
        cd "$repo_name" || continue
        # Pull the latest changes if it's a Git repository
        if [ -d ".git" ]; then
            git pull
        else
            echo "$repo_name exists but is not a Git repository, skipping..."
        fi
        cd ..
    else
        # Clone the repository if it doesn't exist
        echo "Cloning $repo_name from $repo_url..."
        git clone "$repo_url"
    fi
done
