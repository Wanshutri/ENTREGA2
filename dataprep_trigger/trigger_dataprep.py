import os
import base64
import json
import requests
from google.cloud import storage

def trigger_dataprep(event, context):
    # 1) Decodificar mensaje Pub/Sub
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    msg_json = json.loads(pubsub_message)
    # El nombre del archivo se halla en msg_json['name']
    file_name = msg_json['name']

    # 2) Llamar a la API de Dataprep
    flow_id = os.environ['DATAPREP_FLOW_ID']
    output_name = os.environ['DATAPREP_OUTPUT_NAME']
    token = os.environ['DATAPREP_TOKEN']

    url = f"https://api.dataprep.com/v4/jobs"
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    payload = {
        'flowId': flow_id,
        'outputName': output_name,
        # Si tu recipe filtra por prefijo, le puedes pasar el path de este JSON:
        'parameters': {
            'inputPath': f'gs://{os.environ["BUCKET_NAME"]}/{file_name}'
        }
    }
    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code not in (200, 202):
        raise Exception(f"Error al disparar Dataprep: {resp.text}")
    return resp.text