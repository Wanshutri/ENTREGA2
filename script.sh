#!/usr/bin/env bash

# Solicita datos al usuario
read -p "ID de proyecto: " PROJECT_ID
read -p "Nombre del bucket: " BUCKET_NAME

# Crea el bucket si no existe
gsutil mb -p $PROJECT_ID -l us-central1 gs://$BUCKET_NAME/

# Inicializa contador en GCS
echo 0 | gsutil cp - gs://$BUCKET_NAME/index/part_index.txt

# ---------- Crear topic Pub/Sub ----------
gcloud pubsub topics create storage_dataprep_trigger

# Configura el bucket para enviar notificaciones a Pub/Sub
gsutil notification create -t storage_dataprep_trigger -f json -p raw/ gs://$BUCKET_NAME

# Construir y desplegar el contenedor para parquet-downloader
gcloud builds submit --tag gcr.io/$PROJECT_ID/parquet-downloader container_parquet/

gcloud run deploy parquet-downloader \
  --image gcr.io/$PROJECT_ID/parquet-downloader \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars BUCKET_NAME=$BUCKET_NAME,PART_SIZE=1000000 \
  --memory 16Gi \
  --cpu 4 \
  --max-instances 1 \
  --execution-environment gen2 \
  --cpu-throttling

read -p "TOKEN de Dataprep: " TOKEN_DATAPREP
read -p "ID del Flow de Dataprep: " RECIPE_ID

gcloud functions deploy process_file \
  --runtime python39 \
  --trigger-topic storage_dataprep_trigger \
  --entry-point process_file \
  --set-env-vars ACCESS_TOKEN=$TOKEN_DATAPREP,RECIPE_ID=$RECIPE_ID \
  --region us-central1 \
  --source trigger_dataprep

# Scheduler job
gcloud scheduler jobs create http parquet-partial-job \
  --schedule="*/15 * * * *" \
  --uri="$(gcloud run services describe parquet-downloader --region us-central1 --format='value(status.url)')/download" \
  --http-method=GET \
  --time-zone="America/Santiago" \
  --location=us-central1