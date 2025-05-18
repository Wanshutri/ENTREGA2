import os
from flask import Flask
import requests
import pyarrow.dataset as ds
import pyarrow as pa
from google.cloud import storage

app = Flask(__name__)
BUCKET       = os.environ['BUCKET_NAME']
SOURCE_URL   = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2016-01.parquet"
PART_SIZE    = int(os.environ.get('PART_SIZE', '500'))   # filas por parte
STATE_BLOB   = 'raw/part_index.txt'
TMP_PARQUET  = '/tmp/data.parquet'

# Inicializa cliente GCS
client = storage.Client()
bucket = client.bucket(BUCKET)

def download_parquet_once():
    if not os.path.exists(TMP_PARQUET):
        resp = requests.get(SOURCE_URL, stream=True)
        with open(TMP_PARQUET, 'wb') as f:
            for chunk in resp.iter_content(10 * 1024 * 1024):
                if chunk:
                    f.write(chunk)

@app.route('/download', methods=['GET'])
def download_incremental():
    # 1) Leer estado
    state_blob = bucket.blob(STATE_BLOB)
    last_index = int(state_blob.download_as_text()) if state_blob.exists() else 0
    batch_index = last_index  # 0-based

    # 2) Asegura parquet local
    download_parquet_once()

    # 3) Prepara el dataset y el scanner
    dataset = ds.dataset(TMP_PARQUET, format='parquet')
    scanner = ds.Scanner.from_dataset(
        dataset,
        batch_size=PART_SIZE,
        use_threads=True
    )

    # 4) Avanza hasta el batch correcto
    for idx, record_batch in enumerate(scanner.to_batches()):
        if idx == batch_index:
            batch = record_batch
            break
    else:
        return f"¡Completo! Solo existen {idx+1} batches de {PART_SIZE} filas.", 200

    # 5) Convierte el RecordBatch a Table y escribe JSONL
    table = pa.Table.from_batches([batch])
    json_path = f'/tmp/data_part_{batch_index+1}.json'
    with pa.OSFile(json_path, 'wb') as sink:
        writer = pa.RecordBatchFileWriter(sink, table.schema)
        writer.write_table(table)
        writer.close()

    # 6) Sube a GCS
    dest_name = f'raw/data_part_{batch_index+1}.json'
    blob = bucket.blob(dest_name)
    blob.chunk_size = 50 * 1024 * 1024
    blob.upload_from_filename(json_path)

    # 7) Actualiza estado para la siguiente invocación
    state_blob.upload_from_string(str(batch_index+1))

    return f"Batch #{batch_index+1} subido -> filas {batch_index*PART_SIZE}–{batch_index*PART_SIZE + batch.num_rows - 1}"
