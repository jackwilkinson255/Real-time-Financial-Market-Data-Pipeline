


helmfile -l name=monitoring destroy
helmfile -l name=monitoring apply


POD_NAME=$(kubectl get pods -l app=ingestion-ingestion -o jsonpath="{.items[0].metadata.name}") && \
kubectl port-forward $POD_NAME 8080:8080 & \
sleep 2 && \
open http://localhost:8080