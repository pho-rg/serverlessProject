from flask import Flask, jsonify, request
from flask_cors import CORS
from azure.storage.blob import BlobServiceClient
import os
import json
from dotenv import dotenv_values

config = dotenv_values(".env")

# Flask app setup
app = Flask(__name__)
CORS(app)  # Enable CORS for frontend communication

# Azure Blob Storage Configuration
AZURE_STORAGE_CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
CONTAINER_NAME = os.environ["CONTAINER_NAME"]
CONTAINER_CSV = os.environ["CONTAINER_CSV"]

blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)
container_client_csv = blob_service_client.get_container_client(CONTAINER_CSV)

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
    
# Endpoint to list available CSV files in the container
@app.route('/csv/files', methods=['GET'])
def list_csv_files():
    """Fetch the list of CSV files in the Azure container."""
    blob_csv_list = container_client_csv.list_blobs()
    csv_files = [blob.name for blob in blob_csv_list if blob.name.endswith('.csv')]
    return jsonify(csv_files)

# Endpoint to push CSV file
@app.route('/csv/upload', methods=['POST'])
def upload_csv():
    """Upload a CSV file to Azure Blob Storage."""
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file part"}), 400

        file = request.files['file']

        if file.filename == '':
            return jsonify({"error": "No selected file"}), 400

        if not file.filename.endswith('.csv'):
            return jsonify({"error": "Only CSV files are allowed"}), 400

        # Upload to Azure Blob Storage
        blob_client = container_client_csv.get_blob_client(file.filename)
        blob_client.upload_blob(file.read(), overwrite=True)

        return jsonify({"message": f"File {file.filename} uploaded successfully"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)