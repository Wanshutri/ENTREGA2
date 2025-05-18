import os
from flask import Flask, request
import requests
import pyarrow.parquet as pq
import pyarrow as pa
from google.cloud import storage

app = Flask(__name__)
BUCKET = os.environ['BUCKET_NAME']
SOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2016-01.parquet"
PART_SIZE = int(os.environ.get('PART_SIZE', '15000'))  # filas por parte

@app.route('/download', methods=['GET'])
def download_partial_and_upload():
    # Índice de parte desde query param: ?part=1
    part_index = int(request.args.get('part', '1'))

    # Descargar parquet a temporal (solo primera vez o cache previo)
    parquet_path = '/tmp/data.parquet'
    if not os.path.exists(parquet_path):
        resp = requests.get(SOURCE_URL, stream=True)
        with open(parquet_path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=10*1024*1024):
                if chunk:
                    f.write(chunk)

    # Leer esquema y calcular rango de filas
    parquet_file = pq.ParquetFile(parquet_path)
    total_rows = parquet_file.metadata.num_rows
    start = (part_index - 1) * PART_SIZE
    end = min(start + PART_SIZE, total_rows)

    if start >= total_rows:
        return f"No hay más partes. Total filas: {total_rows}", 400

    # Leer solo el rango de filas
    table = parquet_file.read_row_group(start // parquet_file.metadata.row_group(0).num_rows,
                                       columns=None)
    # Nota: para múltiples row groups puede combinar lógicas más complejas.

    # Convertir a JSONL
    json_path = f'/tmp/data_part_{part_index}.json'
    with pa.OSFile(json_path, 'wb') as sink:
        writer = pa.RecordBatchFileWriter(sink, table.schema)
        writer.write_table(table)
        writer.close()

    # Subir JSONL a GCS
    client = storage.Client()
    blob = client.bucket(BUCKET).blob(f'raw/data_part_{part_index}.json')
    blob.chunk_size = 50 * 1024 * 1024
    blob.upload_from_filename(json_path)

    return f"Parte {part_index} (filas {start}-{end}) subida a gs://{BUCKET}/raw/data_part_{part_index}.json"