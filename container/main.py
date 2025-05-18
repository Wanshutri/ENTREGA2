import os
from flask import Flask
import requests
import pyarrow.parquet as pq
import pyarrow as pa
from google.cloud import storage

app = Flask(__name__)
BUCKET = os.environ['BUCKET_NAME']
# URL siempre inmutable:
SOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2016-01.parquet"
PART_SIZE = int(os.environ.get('PART_SIZE', '5000'))  # filas por parte
STATE_BLOB = 'raw/part_index.txt'

client = storage.Client()
bucket = client.bucket(BUCKET)

@app.route('/download', methods=['GET'])
def download_incremental():
    # 1) Leer índice actual de GCS
    state = bucket.blob(STATE_BLOB)
    if state.exists():
        last_index = int(state.download_as_text())
    else:
        last_index = 0
    part_index = last_index + 1

    # 2) Descargar parquet (solo la primera vez)
    parquet_path = '/tmp/data.parquet'
    if not os.path.exists(parquet_path):
        resp = requests.get(SOURCE_URL, stream=True)
        with open(parquet_path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=10*1024*1024):
                if chunk:
                    f.write(chunk)

    # 3) Calcular ventanas según PART_SIZE
    parquet_file = pq.ParquetFile(parquet_path)
    total_rows = parquet_file.metadata.num_rows
    start = (part_index - 1) * PART_SIZE
    if start >= total_rows:
        return f"¡Completo! Total filas: {total_rows}", 200

    # 4) Leer sólo el row group correspondiente
    row_group_size = parquet_file.metadata.row_group(0).num_rows
    group_idx = start // row_group_size
    table = parquet_file.read_row_group(group_idx)

    # 5) Convertir fragmento a JSONL
    json_path = f'/tmp/data_part_{part_index}.json'
    with pa.OSFile(json_path, 'wb') as sink:
        writer = pa.RecordBatchFileWriter(sink, table.schema)
        writer.write_table(table)
        writer.close()

    # 6) Subir a GCS
    blob = bucket.blob(f'raw/data_part_{part_index}.json')
    blob.chunk_size = 50 * 1024 * 1024
    blob.upload_from_filename(json_path)

    # 7) Actualizar contador en GCS
    state.upload_from_string(str(part_index))

    return f"Parte {part_index} subida (filas {start}-{start + table.num_rows -1})."
