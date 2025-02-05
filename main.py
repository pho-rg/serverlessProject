from flask import Flask, jsonify
from flask_cors import CORS
from azure.storage.blob import BlobServiceClient
import os
import json
from dotenv import load_dotenv

# Flask app setup
app = Flask(__name__)
CORS(app)  # Enable CORS for frontend communication

# Azure Blob Storage Configuration
AZURE_STORAGE_CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
CONTAINER_NAME = os.environ["CONTAINER_NAME"]

blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# Endpoint to list available JSON files in the container
@app.route('/files', methods=['GET'])
def list_json_files():
    """Fetch the list of JSON files in the Azure container."""
    blob_list = container_client.list_blobs()
    json_files = [blob.name for blob in blob_list if blob.name.endswith('.json')]
    return jsonify(json_files)

# Endpoint to retrieve JSON content
@app.route('/file/<filename>', methods=['GET'])
def get_json_file(filename):
    """Fetch JSON content from the specified file in Azure Blob Storage."""
    try:
        blob_client = container_client.get_blob_client(filename)
        json_data = blob_client.download_blob().readall()
        return jsonify(json.loads(json_data))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)