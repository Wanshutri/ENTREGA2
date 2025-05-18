import os
import json
import requests
import base64
from google.cloud import storage

def process_file(event, context):
    """Esta función es activada cuando se recibe un mensaje de Pub/Sub."""
    
    # Extraemos los datos del mensaje
    pubsub_message = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    
    # Extraemos la información del archivo que activó la notificación
    bucket_name = pubsub_message.get('bucket')
    file_name = pubsub_message.get('name')

    if not bucket_name or not file_name:
        print("Error: bucket o name no están presentes en el mensaje.")
        return
    
    print(f"Se ha detectado un cambio en el archivo: {file_name} en el bucket: {bucket_name}")
    
    # Si necesitas invocar otro proceso como Trifacta, puedes hacerlo aquí.
    trigger_dataprep()

def trigger_dataprep():
    """Función para invocar el flujo de Dataprep a través de su API"""
    
    # Aquí puedes invocar la API de Dataprep o cualquier otra lógica que necesites.
    url = 'https://api.clouddataprep.com/v4/jobGroups'
    token = os.getenv('ACCESS_TOKEN')
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    data = {
        "wrangledDataset": {
            "id": os.getenv('RECIPE_ID') # Cambia esto por tu 'recipe-id' real
        }
    }

    response = requests.post(url, json=data, headers=headers)
    
    if response.status_code == 200:
        print("Job ejecutado con éxito.")
    else:
        print(f"Error al ejecutar el job ({response.status_code}): {response.text}")
        exit(1)