helmfile -l name=ingestion destroy
helmfile -l name=ingestion apply

kubectl logs -f -l app=ingestion-ingestion