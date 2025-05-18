import base64
import json
import os
import requests
from functions_framework import cloud_event

@cloud_event
def trigger_dataprep(cloud_event):
    message = cloud_event.data.get("message", {})
    if "data" not in message:
        print("No data in Pub/Sub message")
        return "No data in Pub/Sub message", 400

    try:
        decoded_data = base64.b64decode(message["data"]).decode("utf-8")
        file_info = json.loads(decoded_data)
        file_name = file_info["name"]
    except Exception as e:
        print(f"Error decoding message data: {e}")
        return f"Error decoding message data: {e}", 400

    url = "https://api.dataprep.com/v4/jobs"
    headers = {
        "Authorization": f"Bearer {os.environ['DATAPREP_TOKEN']}",
        "Content-Type": "application/json"
    }
    payload = {
        "flowId": os.environ["DATAPREP_FLOW_ID"],
        "outputName": os.environ["DATAPREP_OUTPUT_NAME"],
        "parameters": {
            "inputPath": f"gs://{os.environ['BUCKET_NAME']}/raw/{file_name}"
        }
    }

    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code in (200, 202):
        print("Dataprep job triggered successfully.")
        return "OK", 200
    else:
        print(f"Error triggering Dataprep job: {resp.text}")
        return f"Error triggering Dataprep job: {resp.text}", 500