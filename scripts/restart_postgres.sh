helmfile -l name=storage destroy
kubectl delete pvc data-storage-postgresql-0 -n default
helmfile -l name=storage apply
kubectl port-forward svc/storage-postgresql 5432:5432
