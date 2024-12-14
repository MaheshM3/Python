import requests
from pathlib import Path

# Construct paths to the key and certificate files
key_file_path = (Path(__file__).resolve().parent.parent / "key.key").resolve()
cert_file_path = (Path(__file__).resolve().parent.parent / "cert.pem").resolve()

# Use the key and certificate in the requests call
response = requests.get("https://example.com", cert=(cert_file_path, key_file_path))

print("Response status:", response.status_code)
