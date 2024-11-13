#!/bin/bash

# List of repository clone URLs
repos=(
    "https://github.com/user/repo1.git"
    "https://github.com/user/repo2.git"
    "https://github.com/user/repo3.git"
)

# Base directory where repositories will be stored
base_dir="path/to/your/repos"

# Create base directory if it doesn't exist
mkdir -p "$base_dir"
cd "$base_dir" || exit 1

# Iterate over each repository URL
for repo_url in "${repos[@]}"; do
    # Extract the repo name from the URL (e.g., repo1 from https://github.com/user/repo1.git)
    repo_name=$(basename "$repo_url" .git)

    # Check if the repository directory already exists
    if [ -d "$repo_name" ]; then
        echo "Checking $repo_name for local changes..."

        cd "$repo_name" || continue

        # Option 1: Stash local changes before pulling (without popping them afterward)
        if [[ $(git status --porcelain) ]]; then
            echo "Stashing local changes in $repo_name..."
            git stash --include-untracked
        fi

        # Pull the latest changes
        git pull

        cd ..
    else
        # Clone the repository if it doesn't exist
        echo "Cloning $repo_name from $repo_url..."
        git clone "$repo_url"
    fi
done
