import os
import json
import stat
import urllib.parse
import sys
import requests

class VaultService:
    def __init__(self, file_source=None, url_source=None):
        """Initialize VaultService with local JSON file and remote URL fallback."""
        self.file_path = file_source
        self.url_source = url_source

        self.local_data = {}
        self.remote_data = {}

        # Load local JSON if provided
        if self.file_path:
            self.local_data = self._load_json_file(self.file_path)
            print(f"Loaded local JSON file: {self.file_path}")

        # Load remote JSON if provided
        if self.url_source:
            self.remote_data = self._fetch_json_url(self.url_source)
            print(f"Loaded remote JSON from: {self.url_source}")

    def _is_local_file(self, path):
        """Check if the given path is a local file."""
        return os.path.exists(path) and os.path.isfile(path)

    def _get_file_path(self, file_url):
        """Extract file path from a file:// URL."""
        parsed_url = urllib.parse.urlparse(file_url)
        file_path = parsed_url.path

        if os.name == "nt":  # Windows fix for drive letters
            file_path = file_path.lstrip("/")

        return file_path

    def _load_json_file(self, file_path):
        """Validate and load JSON from a local file."""
        if not os.path.exists(file_path):
            raise ValueError(f"JSON file does not exist: {file_path}")

        # Check file permissions (400 or 600)
        file_stat = os.stat(file_path)
        file_permissions = file_stat.st_mode & 0o777
        if file_permissions not in [0o400, 0o600]:
            raise PermissionError(f"Invalid file permissions for {file_path}. Should be 400 or 600.")

        try:
            with open(file_path, "r", encoding="utf-8") as file:
                return json.load(file)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON file: {file_path}. Error: {e}")

    def _fetch_json_url(self, url):
        """Fetch and parse JSON data from a remote URL or file:// URL."""
        if url.startswith("file://"):
            file_path = self._get_file_path(url)
            return self._load_json_file(file_path)

        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise ValueError(f"Failed to fetch data from URL: {url}. Error: {e}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON from URL: {url}. Error: {e}")

    def get(self, key, default=None):
        """Retrieve a value, prioritizing local file data but falling back to the URL."""
        if key in self.local_data:
            return self.local_data[key]
        return self.remote_data.get(key, default)


# Command-line entry point
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python -m vaultproxy <file.json> <url>")
        sys.exit(1)

    file_source = sys.argv[1]
    url_source = sys.argv[2]

    try:
        vault_service = VaultService(file_source, url_source)
    except Exception as e:
        print(f"Error initializing VaultService: {e}")
        sys.exit(1)

    # Example usage: Fetch a key from VaultService
    test_key = "api_key"
    print(f"Value for '{test_key}': {vault_service.get(test_key, 'Not found')}")