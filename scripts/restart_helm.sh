# Enable error handling for commands
set -e  # Exit immediately if a command exits with a non-zero status
trap 'handle_error $LINENO $?' ERR  # Trap errors and call the error handler

# Error handler function
handle_error() {
    echo "Helm uninstall not found..."
    helm install fdp . --debug
}

if [[ "$1" == "full" ]]; then
    minikube delete
    minikube start
fi

helm uninstall fdp
if [[ "$1" == "update-deps" ]]; then
    helm dependencies update
fi
helm install fdp . --debug
