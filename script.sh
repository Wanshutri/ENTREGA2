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
  --set-env-vars BUCKET_NAME=$BUCKET_NAME,PART_SIZE=1000 \
  --memory 16Gi \
  --cpu 4 \
  --max-instances 1 \
  --execution-environment gen2 \
  --cpu-throttling

# Scheduler job
gcloud scheduler jobs create http parquet-partial-job \
  --schedule="*/15 * * * *" \
  --uri="$(gcloud run services describe parquet-downloader --region us-central1 --format='value(status.url)')/download" \
  --http-method=GET \
  --time-zone="America/Santiago" \
  --location=us-central1

# ---------- ETAPA 2: TRIGGER DATAPREP POR PUBSUB CON CLOUD FUNCTION ----------
# Crea el tópico de Pub/Sub
gcloud pubsub topics create dataprep-trigger-topic

# Configura notificación GCS → Pub/Sub para el prefijo raw/
gsutil notification create \
  -t dataprep-trigger-topic \
  -f json \
  -p raw/ \
  gs://$BUCKET_NAME

# Solicita datos de Dataprep
read -p "ID del Flow de Dataprep: " DATAPREP_ID
read -p "Token de acceso de Dataprep: " DATAPREP_TOKEN

# Obtener número de proyecto
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format='value(projectNumber)')

# Desplegar función Cloud Function con trigger Pub/Sub
gcloud functions deploy dataprep_trigger_function \
  --runtime python310 \
  --trigger-topic dataprep-trigger-topic \
  --entry-point trigger_dataprep \
  --timeout 540 \
  --set-env-vars BUCKET_NAME=$BUCKET_NAME,DATAPREP_FLOW_ID=$DATAPREP_ID,DATAPREP_OUTPUT_NAME=bq_output,DATAPREP_TOKEN=$DATAPREP_TOKEN \
  --region us-central1 \
  --allow-unauthenticated \
  --source=dataprep_trigger_function

echo "Despliegue completado. La función se disparará automáticamente al recibir mensajes en el tópico dataprep-trigger-topic."