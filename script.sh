#!/usr/bin/env bash
read -p "ID de proyecto: " PROJECT_ID
read -p "Nombre del bucket: " BUCKET_NAME

# Crear el bucket si no existe
gsutil mb gs://$BUCKET_NAME/

# Construir y desplegar
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

# Inicializar contador en GCS
echo 0 | gsutil cp - gs://$BUCKET_NAME/raw/part_index.txt

# Crear Scheduler job único
gcloud scheduler jobs create http parquet-partial-job \
  --schedule="*/15 * * * *" \
  --uri="$(gcloud run services describe parquet-downloader --region us-central1 --format='value(status.url)')/download" \
  --http-method=GET \
  --time-zone="America/Santiago" \
  --location=us-central1

# Crea un tema de Pub/Sub
gcloud pubsub topics create dataprep-trigger-topic

# Configura la notificación de creación de objetos JSON en tu bucket
gsutil notification create \
  -t dataprep-trigger-topic \
  -f json \
  gs://$BUCKET_NAME


read -p "Nombre del Flow de dataprep: " DATAPREP_ID
read -p "Token de acceso de dataprep: " DATAPREP_TOKEN

gcloud functions deploy dataprep-trigger/trigger_dataprep \
  --project=$PROJECT_ID \
  --region=us-central1 \
  --entry-point=trigger_dataprep \
  --runtime=python39 \
  --trigger-topic=dataprep-trigger-topic \
  --source=. \
  --set-env-vars \
    BUCKET_NAME=$BUCKET_NAME,\
    DATAPREP_FLOW_ID=$DATAPREP_ID,\
    DATAPREP_OUTPUT_NAME=bq_output,\
    DATAPREP_TOKEN=$DATAPREP_TOKEN