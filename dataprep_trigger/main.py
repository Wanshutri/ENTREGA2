from flask import Flask, request
import base64, json, os, requests

app = Flask(__name__)

@app.route("/", methods=["POST"])
def trigger():
    envelope = request.get_json()
    if not envelope or 'message' not in envelope:
        return "No Pub/Sub message received", 400

    pubsub_message = envelope['message']
    data = base64.b64decode(pubsub_message['data']).decode('utf-8')
    file_name = json.loads(data)['name']

    # Preparar llamada a API Dataprep
    url = "https://api.dataprep.com/v4/jobs"
    headers = {
        'Authorization': f'Bearer {os.environ["DATAPREP_TOKEN"]}',
        'Content-Type': 'application/json'
    }
    payload = {
        'flowId': os.environ["DATAPREP_FLOW_ID"],
        'outputName': os.environ["DATAPREP_OUTPUT_NAME"],
        'parameters': {
            'inputPath': f'gs://{os.environ["BUCKET_NAME"]}/raw/{file_name}'
        }
    }
    resp = requests.post(url, headers=headers, json=payload)
    return ("OK", 200) if resp.status_code in (200, 202) else (resp.text, 500)
