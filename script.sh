#!/usr/bin/env bash
read -p "ID de proyecto: " PROJECT_ID
read -p "Nombre del bucket: " BUCKET_NAME

# Crear el bucket si no existe
gsutil mb gs://$BUCKET_NAME/

# Construir y desplegar
gcloud builds submit --tag gcr.io/$PROJECT_ID/parquet-downloader container/

gcloud run deploy parquet-downloader \
  --image gcr.io/$PROJECT_ID/parquet-downloader \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars BUCKET_NAME=$BUCKET_NAME,PART_SIZE=1000000

# Inicializar contador en GCS echoando 0
echo 0 | gsutil cp - gs://$BUCKET_NAME/raw/part_index.txt

# Crear Scheduler job único
gcloud scheduler jobs create http parquet-partial-job \
  --schedule="0 1 * * *" \
  --uri="$(gcloud run services describe parquet-downloader --region us-central1 --format='value(status.url)')/download" \
  --http-method=GET \
  --time-zone="America/Santiago"
