#!/usr/bin/env bash

# Solicita datos al usuario
read -p "ID de proyecto: " PROJECT_ID
read -p "Nombre del bucket: " BUCKET_NAME

# Crea el bucket si no existe
gsutil mb -p $PROJECT_ID -l us-central1 gs://$BUCKET_NAME/

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

# Inicializa contador en GCS
echo 0 | gsutil cp - gs://$BUCKET_NAME/raw/part_index.txt

# Scheduler job
gcloud scheduler jobs create http parquet-partial-job \
  --schedule="*/15 * * * *" \
  --uri="$(gcloud run services describe parquet-downloader --region us-central1 --format='value(status.url)')/download" \
  --http-method=GET \
  --time-zone="America/Santiago" \
  --location=us-central1


# ---------- ETAPA 2: TRIGGER DATAPREP POR PUBSUB ----------
# Crea el tópico de Pub/Sub
gcloud pubsub topics create dataprep-trigger-topic

# Configura notificación GCS → Pub/Sub
gsutil notification create \
  -t dataprep-trigger-topic \
  -f json \
  -p raw/ \
  gs://$BUCKET_NAME

# Solicita datos de Dataprep
read -p "ID del Flow de Dataprep: " DATAPREP_ID
read -p "Token de acceso de Dataprep: " DATAPREP_TOKEN

# Construir imagen del contenedor que escucha el Pub/Sub
gcloud builds submit --tag gcr.io/$PROJECT_ID/dataprep-trigger dataprep_trigger/

# Crear servicio de Cloud Run que escuche Pub/Sub
gcloud run deploy dataprep-trigger \
  --image gcr.io/$PROJECT_ID/dataprep-trigger \
  --platform managed \
  --region us-central1 \
  --no-allow-unauthenticated \
  --execution-environment gen2 \
  --set-env-vars BUCKET_NAME=$BUCKET_NAME,DATAPREP_FLOW_ID=$DATAPREP_ID,DATAPREP_OUTPUT_NAME=bq_output,DATAPREP_TOKEN=$DATAPREP_TOKEN

# Vincular servicio Cloud Run al tópico Pub/Sub
gcloud run services add-iam-policy-binding dataprep-trigger \
  --region us-central1 \
  --member=serviceAccount:service-$PROJECT_NUMBER@gcp-sa-pubsub.iam.gserviceaccount.com \
  --role=roles/run.invoker

PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format='value(projectNumber)')

gcloud pubsub subscriptions create dataprep-sub \
  --topic=dataprep-trigger-topic \
  --push-endpoint=$(gcloud run services describe dataprep-trigger --region us-central1 --format='value(status.url)') \
  --push-auth-service-account=service-$PROJECT_NUMBER@gcp-sa-pubsub.iam.gserviceaccount.com