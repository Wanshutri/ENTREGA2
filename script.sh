#!/usr/bin/env bash

# Solicita datos al usuario
read -p "ID de proyecto: " PROJECT_ID
read -p "Nombre del bucket: " BUCKET_NAME

# Crea el bucket si no existe
gsutil mb -p $PROJECT_ID -l us-central1 gs://$BUCKET_NAME/

# Inicializa contador en GCS
echo 0 | gsutil cp - gs://$BUCKET_NAME/raw/part_index.txt

# ---------- ETAPA 1: PARQUET DOWNLOADER ----------
# Construir y desplegar el contenedor para parquet-downloader
gcloud builds submit --tag gcr.io/$PROJECT_ID/parquet-downloader container_parquet/

gcloud run deploy parquet-downloader \
  --image gcr.io/$PROJECT_ID/parquet-downloader \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars BUCKET_NAME=$BUCKET_NAME,PART_SIZE=5000 \
  --memory 16Gi \
  --cpu 4 \
  --max-instances 1 \
  --execution-environment gen2 \
  --cpu-throttling

# Scheduler job
gcloud scheduler jobs create http parquet-partial-job \
  --schedule="*/5 * * * *" \
  --uri="$(gcloud run services describe parquet-downloader --region us-central1 --format='value(status.url)')/download" \
  --http-method=GET \
  --time-zone="America/Santiago" \
  --location=us-central1