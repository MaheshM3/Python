#!/bin/bash

# Base directory where repositories will be stored
base_dir="/mnt"

# Create base directory if it doesn't exist
mkdir -p "$base_dir"
cd "$base_dir" || exit 1

# Array of repository URLs (example)
repos=("https://github.com/user/repo1.git" "https://github.com/user/repo2.git")

# Iterate over each repository URL
for repo_url in "${repos[@]}"; do
    # Extract the repo name from the URL (e.g., repo1 from https://github.com/user/repo1.git)
    repo_name=$(basename "$repo_url" .git)

    # Check if the repository directory already exists
    if [ -d "$repo_name" ]; then
        echo "Checking $repo_name for local changes..."

        cd "$repo_name" || continue

        # Stash local changes (if any, including untracked files)
        if [ -n "$(git status --porcelain)" ]; then
            echo "Stashing local changes in $repo_name..."
            git stash save --include-untracked "Auto-stash $(date)"
        fi

        # Fetch the latest updates and prune deleted branches
        echo "Fetching updates for $repo_name..."
        git fetch --prune

        # Delete local branches that no longer exist on the remote
        echo "Cleaning up local branches in $repo_name..."
        git branch -vv | grep ': gone' | awk '{print $1}' | xargs -r git branch -d

        # Pull the latest changes
        echo "Pulling latest changes for $repo_name..."
        git pull

        # Go back to the base directory
        cd ..
    else
        # Clone the repository if it doesn't exist
        echo "Cloning $repo_name from $repo_url..."
        git clone "$repo_url"
    fi
done
