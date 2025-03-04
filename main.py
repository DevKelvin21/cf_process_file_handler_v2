import os
import json
import io
import csv
import base64
import tempfile
import requests
import functions_framework
from google.cloud import firestore, storage
from cloudevents.http import CloudEvent

# Maximum payload size for the API call (1MB).
MAX_PAYLOAD_SIZE = 1048576  # bytes


@functions_framework.cloud_event
def process_file_v2(cloudevent: CloudEvent):
    """
    Cloud Function (Gen2) entry point to process a CSV file.
    Steps:
      1. Download CSV from Cloud Storage.
      2. Get config from Firestore to identify phone columns.
      3. Extract phone numbers and batch them so that each API call payload is below 1MB.
      4. Call the blacklist API in batches and aggregate the "supression" results.
      5. If no suppressed numbers are returned, simply store the original file as cleanFilePath.
      6. Otherwise, process the CSV:
         - Create a clean file (emptying any suppressed phone numbers).
         - Create a blacklisted file (only retaining suppressed numbers per lead, merging duplicates).
      7. Upload generated files to Cloud Storage and update Firestore.
    """
    # --- Step 1. Decode the Pub/Sub message ---
    try:
        data = base64.b64decode(cloudevent.data['message']['data']).decode('utf-8')
        message = json.loads(data)
    except Exception as e:
        print(f"Error decoding message: {e}")
        return

    file_id = message.get("fileId")
    file_name = message.get("fileName")
    bucket_name = message.get("bucket")
    config_doc_path = message.get("configDocumentPath")

    if not all([file_id, file_name, bucket_name, config_doc_path]):
        print("Missing one or more required fields in the message.")
        return

    print(f"Processing fileId: {file_id}, fileName: {file_name}, bucket: {bucket_name}, config path: {config_doc_path}")

    # --- Step 2. Initialize Firestore and Storage clients ---
    fs_client = firestore.Client()
    storage_client = storage.Client()

    # Fetch configuration document from Firestore.
    config_ref = fs_client.document(config_doc_path)
    config_snapshot = config_ref.get()
    if not config_snapshot.exists:
        print(f"Configuration document not found: {config_doc_path}")
        return
    config = config_snapshot.to_dict()

    # Retrieve dynamic configuration details.
    phone_indexes = config.get("phoneColumnIndexes", [])
    has_header = config.get("hasHeaderRow", True)

    # --- Step 3. Download and parse the CSV file ---
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(os.path.join('uploads', file_name))
    try:
        file_content = blob.download_as_bytes()
    except Exception as e:
        print(f"Error downloading file: {e}")
        return

    decoded_file = file_content.decode('utf-8')
    csv_lines = decoded_file.splitlines()
    reader = csv.reader(csv_lines)
    rows = list(reader)
    if not rows:
        print("Empty CSV file.")
        return

    # Separate header from data rows if needed.
    if has_header:
        header = rows[0]
        data_rows = rows[1:]
    else:
        header = None
        data_rows = rows

    # --- Step 4. Extract unique phone numbers ---
    phone_set = set()
    for row in data_rows:
        for idx in phone_indexes:
            if idx < len(row):
                phone = row[idx].strip()
                if phone:
                    phone_set.add(phone)
    phone_list = list(phone_set)
    print(f"Extracted {len(phone_list)} unique phone numbers for API lookup.")

    # --- Step 5. Call the blacklist API in batches ---
    try:
        suppressed_list = call_blacklist_lookup_batched(phone_list)
    except Exception as e:
        print(f"Error calling blacklist API: {e}")
        return

    suppressed_set = set(suppressed_list)
    print(f"Aggregated suppressed numbers: {suppressed_set}")

    # Update Firestore stage.
    config_ref.update({
        "status": {
            "stage": "API_CALLED",
            "lastUpdated": firestore.SERVER_TIMESTAMP
        }
    })

    # --- Special Case: No suppressed numbers ---
    if not suppressed_set:
        print("No suppressed numbers returned. Uploading original file as cleanFilePath.")
        output_bucket_name = os.getenv("OUTPUT_BUCKET")
        if not output_bucket_name:
            print("OUTPUT_BUCKET environment variable is not set.")
            return
        output_bucket = storage_client.bucket(output_bucket_name)
        # Upload the original file content as the clean file.
        base_name, ext = os.path.splitext(file_name)
        clean_file_name = f"{base_name}_clean{ext}"
        clean_blob_name = f"{file_id}/{clean_file_name}"
        clean_blob = output_bucket.blob(clean_blob_name)
        clean_blob.upload_from_string(decoded_file, content_type="text/csv")

        # Update Firestore with output path.
        config_ref.update({
            "results" : {
                "total": len(phone_list),
                "clean": len(phone_list),
                "dnc": 0
            },
            "outputFiles": {
                "cleanFilePath": clean_blob_name,
                "blacklistedFilePath": ""  # No blacklisted file produced.
            },
            "status": {
                "stage": "DONE",
                "lastUpdated": firestore.SERVER_TIMESTAMP
            }
        })
        print(f"Processed file_id={file_id}. Clean file uploaded to: {clean_blob_name}")
        return

    # --- Step 6. Process CSV rows for clean and blacklisted outputs ---
    clean_rows = []
    suppressed_dict = {}  # Group suppressed rows by lead_id (assumed to be in column 0)

    # Add header if present.
    if header:
        clean_rows.append(header)
        suppressed_header = header[:]  # Copy for suppressed file
    else:
        suppressed_header = None

    for row in data_rows:
        clean_row = list(row)       # For clean file: clear suppressed phones.
        suppressed_row = list(row)  # For blacklisted file: retain only suppressed phones.
        row_has_suppressed = False

        for idx in phone_indexes:
            if idx < len(row):
                phone_val = row[idx].strip()
                if phone_val in suppressed_set:
                    # For clean file, empty the cell.
                    clean_row[idx] = ""
                    # For suppressed file, keep the suppressed number.
                    suppressed_row[idx] = phone_val
                    row_has_suppressed = True
                else:
                    # Clear non-suppressed phone cells in the suppressed file.
                    suppressed_row[idx] = ""
        clean_rows.append(clean_row)

        # Group rows with suppressed numbers by lead_id.
        if row_has_suppressed:
            lead_id = row[0] if len(row) > 0 else None
            if lead_id in suppressed_dict:
                existing_row = suppressed_dict[lead_id]
                for idx in phone_indexes:
                    # Merge suppressed phone numbers for the same lead.
                    if idx < len(row) and not existing_row[idx] and suppressed_row[idx]:
                        existing_row[idx] = suppressed_row[idx]
                suppressed_dict[lead_id] = existing_row
            else:
                suppressed_dict[lead_id] = suppressed_row

    # Build suppressed file rows.
    suppressed_rows = []
    if suppressed_header:
        suppressed_rows.append(suppressed_header)
    suppressed_rows.extend(suppressed_dict.values())

    # --- Step 7. Write the output CSV files in memory ---
    clean_csv_output = io.StringIO()
    writer_clean = csv.writer(clean_csv_output)
    for row in clean_rows:
        writer_clean.writerow(row)
    clean_csv_content = clean_csv_output.getvalue()

    suppressed_csv_output = io.StringIO()
    writer_suppressed = csv.writer(suppressed_csv_output)
    if suppressed_header:
        writer_suppressed.writerow(suppressed_header)
    for row in (list(suppressed_dict.values()) if not suppressed_header else suppressed_rows[1:]):
        writer_suppressed.writerow(row)
    suppressed_csv_content = suppressed_csv_output.getvalue()

    # --- Step 8. Upload generated files to Cloud Storage ---
    output_bucket_name = os.getenv("OUTPUT_BUCKET")
    if not output_bucket_name:
        print("OUTPUT_BUCKET environment variable is not set.")
        return
    output_bucket = storage_client.bucket(output_bucket_name)

    base_name, ext = os.path.splitext(file_name)
    clean_file_name = f"{base_name}_clean{ext}"
    suppressed_file_name = f"{base_name}_blacklisted{ext}"

    clean_blob_name = f"{file_id}/{clean_file_name}"
    suppressed_blob_name = f"{file_id}/{suppressed_file_name}"

    clean_blob = output_bucket.blob(clean_blob_name)
    suppressed_blob = output_bucket.blob(suppressed_blob_name)

    clean_blob.upload_from_string(clean_csv_content, content_type="text/csv")
    suppressed_blob.upload_from_string(suppressed_csv_content, content_type="text/csv")

    output_paths = {
        "cleanFilePath": clean_blob_name,
        "blacklistedFilePath": suppressed_blob_name
    }

    # --- Step 9. Update Firestore with output file paths and final status ---
    config_ref.update({
        "results": {
            "total": len(phone_list),
            "clean": len(phone_list) - len(suppressed_set),
            "dnc": len(suppressed_set)
        },
        "outputFiles": output_paths,
        "status": {
            "stage": "DONE",
            "lastUpdated": firestore.SERVER_TIMESTAMP
        }
    })

    print(f"Processed file_id={file_id}. Clean file uploaded to: {clean_blob_name}")
    print(f"Processed file_id={file_id}. Blacklisted file uploaded to: {suppressed_blob_name}")

def call_blacklist_lookup_batched(phone_list):
    """
    Breaks the phone_list into batches such that each JSON payload ({"phones": batch})
    is less than MAX_PAYLOAD_SIZE (1MB). It then calls the API for each batch and
    aggregates the suppressed numbers.
    """
    batches = []
    current_batch = []
    for phone in phone_list:
        current_batch.append(phone)
        payload = {"phones": current_batch}
        payload_str = json.dumps(payload)
        # Check size in bytes; if over the limit, remove the last phone and finalize the batch.
        if len(payload_str.encode('utf-8')) > MAX_PAYLOAD_SIZE:
            current_batch.pop()
            if current_batch:
                batches.append(current_batch)
            current_batch = [phone]
    if current_batch:
        batches.append(current_batch)

    suppressed = set()
    for batch in batches:
        suppressed_batch = call_blacklist_lookup(batch)
        suppressed.update(suppressed_batch)
    return list(suppressed)

def call_blacklist_lookup(phone_batch):
    """
    Calls the external blacklist lookup API with a JSON payload containing the given phone_batch.
    API URL example:
      https://api.blacklistalliance.net/bulklookup?key=HxWQvKK2g8MyDz7XFGZN&ver=v1&resp=json
    Returns the list of suppressed phone numbers from the "supression" key.
    """
    api_key = os.getenv("BLACKLIST_API_KEY")
    api_url = f"https://api.blacklistalliance.net/bulklookup?key={api_key}&ver=v1&resp=json"
    payload = {"phones": phone_batch}
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    response = requests.post(api_url, json=payload, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Blacklist API call failed: {response.status_code} - {response.text}")
    response_json = response.json()
    # Retrieve suppressed numbers from the API response.
    return response_json.get("supression", [])
