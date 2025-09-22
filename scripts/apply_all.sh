helmfile -l name=streaming -l name=storage apply
helmfile -l name=processing -l name=monitoring apply
helmfile -l name=ingestion apply

