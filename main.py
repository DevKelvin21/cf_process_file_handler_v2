import os
import json
import io
import csv
import base64
import zipfile
import tempfile
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
    data = base64.b64decode(cloudevent.data['message']['data']).decode('utf-8')
    message = json.loads(data)

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

    phone_indexes = config.get("phoneColumnIndexes", [])
    has_header = config.get("hasHeaderRow", False)
    phone_headers = config.get('phoneColumns', [])

    # Download the file from GCS
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(os.path.join('uploads', file_name))
    file_content = blob.download_as_bytes()

    # --- Step 3. Read original CSV and determine non-phone columns ---
    decoded_file = file_content.decode('utf-8')
    reader = csv.reader(decoded_file.splitlines())
    csv_rows = list(reader)
    if not csv_rows:
        print("Empty CSV file.")
        return

    original_headers = csv_rows[0]
    # Identify non-phone column indices
    non_phone_indices = [i for i in range(len(original_headers)) if i not in phone_indexes]
    non_phone_headers = [original_headers[i] for i in non_phone_indices]

    # --- Step 4. Expand rows into one row per phone ---
    # Each expanded row will include a hidden "phone_order" so that we can later restore the order.
    expanded_rows = []
    for row in csv_rows[1:]:
        non_phone_data = [row[i] for i in non_phone_indices]
        for order, idx in enumerate(phone_indexes):
            if idx < len(row):
                phone = row[idx].strip()
                if phone:
                    expanded_rows.append({
                        'phone': phone,
                        'phone_order': order,
                        'non_phone_data': non_phone_data
                    })

    with tempfile.NamedTemporaryFile(mode='w+', delete=False, newline='') as tmp_expanded:
        writer = csv.writer(tmp_expanded)
        writer.writerow(['phone', 'phone_order'] + non_phone_headers)
        for row in expanded_rows:
            writer.writerow([row['phone'], row['phone_order']] + row['non_phone_data'])
        expanded_file_path = tmp_expanded.name

    config_ref.update({
        "status": {
            "stage": "PREPARED",
            "lastUpdated": firestore.SERVER_TIMESTAMP
            }
    })
    # --- Step 5. Process the expanded file via the blacklist API ---
    processed_zip_path = call_blacklist_api(expanded_file_path)

    config_ref.update({
            "status": {
                "stage": "API_CALLED",
                "lastUpdated": firestore.SERVER_TIMESTAMP
                }
        })
    # --- Step 6. Decompress the ZIP and build results + prepare for merging ---
    # We'll accumulate each CSV file's raw content (to be uploaded individually)
    # and also collect the rows (with their file base) for merging.
    results = {}           # key: result_filename, value: CSV content as string
    processed_rows_all = []  # list of tuples: (file_base, row)
    file_names_set = set()

    with zipfile.ZipFile(processed_zip_path, 'r') as zip_ref:
        for csv_filename in zip_ref.namelist():
            if csv_filename.endswith('.csv'):
                file_base = os.path.splitext(os.path.basename(csv_filename))[0]
                file_names_set.add(file_base)
                # Read entire file content as string and store in results
                raw_bytes = zip_ref.read(csv_filename)
                csv_content = raw_bytes.decode('utf-8')
                results[file_base] = csv_content

                # Also, parse rows for merging.
                with zip_ref.open(csv_filename) as csvfile:
                    reader = csv.reader(io.TextIOWrapper(csvfile, 'utf-8'))
                    try:
                        next(reader)  # Skip header
                    except StopIteration:
                        continue
                    for row in reader:
                        # Attach the file_base to each row for later grouping.
                        processed_rows_all.append((file_base, row))

    
    # --- Step 7. Merge processed rows to re-create the original lead rows ---
    # Group rows by non-phone data. For each group, for each file (ordered),
    # we place the phone numbers in the order indicated by phone_order.
    merge_dict = {}  # key: non_phone_data tuple, value: dict mapping file_base -> list of phones
    for file_base, row in processed_rows_all:
        phone = row[0].strip()
        try:
            phone_order = int(row[1])
        except ValueError:
            continue
        non_phone_data = tuple(row[2:])
        if non_phone_data not in merge_dict:
            merge_dict[non_phone_data] = {}
        if file_base not in merge_dict[non_phone_data]:
            merge_dict[non_phone_data][file_base] = ["" for _ in range(len(phone_headers))]
        merge_dict[non_phone_data][file_base][phone_order] = phone

    ordered_files = sorted(list(file_names_set))
    merged_rows = []
    for non_phone_data, file_data in merge_dict.items():
        merged_row = list(non_phone_data)
        for file_base in ordered_files:
            phones = file_data.get(file_base, ["" for _ in range(len(phone_headers))])
            merged_row.extend(phones)
        merged_rows.append(merged_row)

    # Build final merged CSV content as a string.
    final_header = non_phone_headers[:]
    for file_base in ordered_files:
        for header in phone_headers:
            final_header.append(f"{file_base}_{header}")
    merged_output = io.StringIO()
    writer = csv.writer(merged_output)
    writer.writerow(final_header)
    for row in merged_rows:
        writer.writerow(row)
    merged_csv_content = merged_output.getvalue()
    results["merged"] = merged_csv_content

    # --- Step 8. Upload each resulting CSV file to the output bucket ---
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
        output_paths[result_filename] = f"{file_id}/{file_name}_{result_filename}"

    # Update the Firestore document with output file paths and processing status
    config_ref.update({
        "outputFiles": {
            "cleanFilePath": output_paths.get("all_clean.csv", ""),
            "invalidFilePath": output_paths.get("invalid.csv", ""),
            "dncFilePath": output_paths.get("federal_dnc.csv", "")
        },
        "status": {
            "stage": "DONE",
            "lastUpdated": firestore.SERVER_TIMESTAMP
            }
    })
    # Clean up temporary files.
    os.remove(expanded_file_path)
    os.remove(processed_zip_path)
    print(f"Processed file_id={file_id}. Output files uploaded: {output_paths}")

def call_blacklist_api(expanded_file_path):
    """
    Calls the external blacklist API with the expanded CSV file.
    It sends the CSV file along with the required parameters and returns the path to the ZIP file response.
    """
    blacklist_api_url = "https://api.blacklistalliance.net/bulk/upload"

    # Read CSV file
    with open(expanded_file_path, 'r', encoding="utf-8-sig") as f:
        csv_payload = f.read()

    file_name = os.path.basename(expanded_file_path)
    file_tuple = (
        file_name,
        io.BytesIO(csv_payload.encode("utf-8-sig")),  # Ensure correct encoding
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
        "key": os.getenv("BLACKLIST_API_KEY"),
        "colnum": "1"
    }
    headers = {"accept": "application/zip"}

    # Send API request
    response = requests.post(blacklist_api_url, data=payload, files=files, headers=headers)

    # Debug response
    print(f"API Response Code: {response.status_code}")
    if response.status_code != 200:
        print(f"API Error Response: {response.text}")
        raise Exception(f"Blacklist API call failed: {response.status_code} - {response.text}")

    # Save the ZIP content to a temporary file
    tmp_zip = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')
    with open(tmp_zip.name, 'wb') as f:
        f.write(response.content)
    return tmp_zip.name
