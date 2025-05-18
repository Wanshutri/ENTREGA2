import os
import json
import requests
import base64
import logging
import time
from flask import Request, abort
from google.cloud import storage

# Configuración básica de logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s'
)
logger = logging.getLogger("process_file")

# Tiempo de espera fijo antes de continuar (en segundos)
FIXED_WAIT_SECONDS = 60  # 1 minuto

def process_file(request: Request):
    """
    Función HTTP que recibe notificaciones de Pub/Sub vía JSON.
    Espera un tiempo fijo antes de invocar Dataprep para asegurar que el archivo
    raw/data.json esté disponible y completamente escrito.
    """

    logger.debug("Entrando en process_file")
    raw_body = request.get_data(as_text=True)
    logger.debug(f"Cuerpo bruto de la petición: {raw_body}")

    envelope = request.get_json(silent=True)
    if not envelope or 'message' not in envelope:
        logger.error(f"Bad Request: envelope inválido o no contiene 'message': {envelope}")
        abort(400, 'Bad Request: se esperaba un JSON con key "message".')

    msg = envelope['message']
    data_b64 = msg.get('data', '')
    if not data_b64:
        logger.error(f"Bad Request: 'message.data' vacío en message: {msg}")
        abort(400, 'Bad Request: "message.data" no contiene datos.')

    try:
        payload_bytes = base64.b64decode(data_b64)
        pubsub_message = json.loads(payload_bytes.decode('utf-8'))
        logger.info(f"Pub/Sub parsed: {pubsub_message}")
    except Exception as e:
        logger.exception("Error al decodificar/parsing del mensaje")
        abort(400, f'Error al decodificar/parsing del mensaje: {e}')

    bucket_name = pubsub_message.get('bucket')
    file_name = pubsub_message.get('name')
    if not bucket_name or not file_name:
        logger.error(f"Bad Request: faltan campos en pubsub_message: {pubsub_message}")
        abort(400, 'Bad Request: faltan "bucket" o "name" en el mensaje.')

    logger.info(f"Detectado cambio en: {file_name} en bucket: {bucket_name}")

    # Espera fija antes de continuar
    logger.debug(f"Esperando {FIXED_WAIT_SECONDS} segundos para asegurar disponibilidad del archivo...")
    time.sleep(FIXED_WAIT_SECONDS)

    # Invocamos Dataprep
    try:
        trigger_dataprep()
    except Exception as e:
        logger.exception("trigger_dataprep falló")
        abort(500, f"Error interno al invocar Dataprep: {e}")

    return ('OK', 200)


def trigger_dataprep():
    """
    Invoca el flujo de Dataprep a través de su API.
    Usa las variables de entorno ACCESS_TOKEN y RECIPE_ID.
    """
    logger.debug("Entrando en trigger_dataprep")

    url = 'https://api.clouddataprep.com/v4/jobGroups'
    token = os.getenv('ACCESS_TOKEN')
    raw_id = os.getenv('RECIPE_ID')

    if not token or not raw_id:
        logger.error("Faltan ACCESS_TOKEN o RECIPE_ID en variables de entorno")
        raise RuntimeError("Faltan ACCESS_TOKEN o RECIPE_ID en las variables de entorno.")

    try:
        recipe_id = int(raw_id)
    except ValueError:
        logger.exception(f"RECIPE_ID inválido: {raw_id}")
        raise RuntimeError(f"RECIPE_ID inválido: {raw_id}")

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    data = {"wrangledDataset": {"id": recipe_id}}

    logger.debug(f"Haciendo POST a Dataprep: URL={url}, data={data}")
    response = requests.post(url, json=data, headers=headers)
    logger.debug(f"Respuesta de Dataprep: status={response.status_code}, body={response.text}")

    if response.status_code == 200:
        logger.info("Job ejecutado con éxito en Dataprep.")
    else:
        logger.error(f"Error al ejecutar el job ({response.status_code}): {response.text}")
        raise RuntimeError(f"Falló el trigger de Dataprep: {response.status_code}")