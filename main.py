import os
import json
import io
import csv
import zipfile
import requests
import functions_framework
from google.cloud import firestore, storage
from cloudevents.http import CloudEvent


@functions_framework.cloud_event
def process_file_v2(cloudevent: CloudEvent):
    """
    Cloud Function (v2) entry point to process a file based on a Pub/Sub message.
    Expected Pub/Sub message JSON (in cloudevent.data) should contain:
      - fileId
      - fileName
      - bucket
      - configDocumentPath  (e.g., "scrubFilesConfig/<docID>")
    """
    # Extract and decode the Pub/Sub message data
    data = cloudevent.data
    print(f"Received cloudevent data: {data}")
    
    message = json.loads(data)
    
    print(f"Decoded message: {message}")

    file_id = message.get("fileId")
    file_name = message.get("fileName")
    bucket_name = message.get("bucket")
    config_doc_path = message.get("configDocumentPath")

    print(f"fileId: {file_id}, fileName: {file_name}, bucket: {bucket_name}, configDocumentPath: {config_doc_path}")

    if not all([file_id, file_name, bucket_name, config_doc_path]):
        print("Missing one or more required fields in the message.")
        return

    # Initialize Firestore and Storage clients
    fs_client = firestore.Client()
    storage_client = storage.Client()

    # Fetch configuration from Firestore
    config_ref = fs_client.document(config_doc_path)
    config_snapshot = config_ref.get()
    if not config_snapshot.exists:
        print(f"Configuration document not found: {config_doc_path}")
        return
    config = config_snapshot.to_dict()

    phone_columns = config.get("phoneColumns", [])
    has_header = config.get("hasHeaderRow", False)

    # Download the file from GCS
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    file_content = blob.download_as_text()

    # Process the CSV content
    lines = file_content.splitlines()
    if has_header and lines:
        # Remove the header (or process it as needed)
        header = lines.pop(0)
    rows = list(csv.reader(lines))

    # Expand rows based on phone columns
    expanded_rows = []
    for row in rows:
        for col_idx in phone_columns:
            if col_idx < len(row):
                new_row = list(row)
                primary_phone = new_row[col_idx]
                expanded_row = [primary_phone] + new_row
                expanded_rows.append(expanded_row)

    # Batch expanded rows into a single CSV payload
    output_buffer = io.StringIO()
    writer = csv.writer(output_buffer)
    for row in expanded_rows:
        writer.writerow(row)
    csv_payload = output_buffer.getvalue()

    # Call the Blacklist API with the CSV payload
    blacklist_api_url = "https://api.blacklistalliance.net/bulk/upload"
    file_tuple = (
        file_name,
        io.BytesIO(csv_payload.encode("utf-8")),
        "text/csv"
    )
    files = {"file": file_tuple}
    payload = {
        "filetype": "csv",
        "download_carrier_data": "false",
        "download_invalid": "true",
        "download_no_carrier": "false",
        "download_wireless": "false",
        "download_federal_dnc": "true",
        "splitchar": ",",
        "key": os.getenv("BLACKLIST_API_KEY", "YOUR_API_KEY"),
        "colnum": "1"
    }
    headers = {
        "accept": "application/zip"
    }
    response = requests.post(blacklist_api_url, data=payload, files=files, headers=headers)
    if response.status_code != 200:
        print(f"Blacklist API call failed: {response.status_code} - {response.text}")
        return

    # Process the returned ZIP file from the API
    zip_bytes = response.content
    results = {}
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zfile:
        for name in zfile.namelist():
            file_content_zip = zfile.read(name).decode("utf-8")
            # Deduplicate lines (naively using a set)
            deduped = "\n".join(set(file_content_zip.splitlines()))
            results[name] = deduped

    # Write each resulting CSV to an output bucket
    output_bucket_name = os.getenv("OUTPUT_BUCKET")
    if not output_bucket_name:
        print("OUTPUT_BUCKET environment variable is not set.")
        return
    output_bucket = storage_client.bucket(output_bucket_name)
    output_paths = {}
    for result_filename, content in results.items():
        output_blob_name = f"{file_id}/{file_name}_{result_filename}"
        out_blob = output_bucket.blob(output_blob_name)
        out_blob.upload_from_string(content, content_type="text/csv")
        output_paths[result_filename] = f"gs://{output_bucket_name}/{file_id}/{file_name}_{result_filename}"

    # Update the Firestore document with output file paths and processing status
    config_ref.update({
        "outputFiles": {
            "cleanFilePath": output_paths.get(f"{file_name}_all_clean.csv", ""),
            "invalidFilePath": output_paths.get(f"{file_name}_invalid.csv", ""),
            "dncFilePath": output_paths.get(f"{file_name}_federal_dnc.csv", "")
        },
        "status": {"stage": "DONE"}
    })

    print(f"Processed file_id={file_id}. Output files uploaded: {output_paths}")
