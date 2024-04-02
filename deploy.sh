gcloud run jobs deploy hadoop-to-vertex-connector-test \
    --source . \
    --tasks 1 \
    --set-env-vars INPUT_FILE="src/configs/input.json" \
    --set-env-vars LOG_LEVEL="DEBUG" \
    --set-env-vars LOG_FORMAT="CLOUD" \
    --max-retries 0 \
    --region us-central1 \
    --project thinking-cacao-418609 \
    --quiet
