gcloud functions deploy vwt-d-gew1-fttx-asbuilt-producer-func \
  --entry-point=fileprocessing \
  --runtime=python37 \
  --project=vwt-d-gew1-fttx-asbuilt \
  --region=europe-west1 \
  --timeout=540s \
  --trigger-resource=vwt-d-gew1-fttx-asbuilt-inbox-stg \
  --trigger-event=google.storage.object.finalize \
  --service-account=vwt-d-gew1-fttx-asbuilt@appspot.gserviceaccount.com
