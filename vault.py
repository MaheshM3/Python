import os
import json
import stat
import urllib.parse

class VaultService:
    def __init__(self):
        """Initialize and validate the VAULT_SERVICE environment variable."""
        self.vault_service = os.getenv("VAULT_SERVICE")

        if not self.vault_service:
            raise ValueError("VAULT_SERVICE environment variable is not set.")

        if self.vault_service.startswith("file://"):
            self.file_path = self._get_file_path(self.vault_service)
            self._validate_json_file(self.file_path)
            print(f"Using JSON file: {self.file_path}")
        else:
            self.file_path = None
            print(f"Using URL: {self.vault_service}")

    def _get_file_path(self, file_url):
        """Extract the file path from a file:// URL."""
        parsed_url = urllib.parse.urlparse(file_url)
        file_path = parsed_url.path  # Extract actual file path

        if os.name == "nt":  # Windows fix for drive letters
            file_path = file_path.lstrip("/")  # Remove leading slash for Windows drive paths

        return file_path

    def _validate_json_file(self, file_path):
        """Validate that the given file contains valid JSON and has the correct permissions."""
        if not os.path.exists(file_path):
            raise ValueError(f"JSON file does not exist: {file_path}")

        # Check file permissions (400 or 600)
        file_stat = os.stat(file_path)
        file_permissions = file_stat.st_mode & 0o777
        if file_permissions not in [0o400, 0o600]:
            raise PermissionError(
                f"Invalid file permissions for {file_path}. Should be 400 or 600."
            )

        # Read and validate JSON
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                json.load(file)  # Attempt to parse JSON
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON file: {file_path}. Error: {e}")

# Example Usage
try:
    vault_service = VaultService()  # Initializes and validates
except Exception as e:
    print(f"Error: {e}")