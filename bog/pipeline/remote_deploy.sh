# remote_deploy.sh

# Define parameters
LOCATION=us-central1
PROJECT_ID=southern-field-305921
REPOSITORY=debit-docker-repo
IMAGE_NAME=bog-remote
REGISTRY_IMAGE_NAME="$LOCATION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/$IMAGE_NAME"

# Build Docker image locally and then push to registry
docker build -f Dockerfile.remote -t $IMAGE_NAME .
docker tag $IMAGE_NAME $REGISTRY_IMAGE_NAME
docker push $REGISTRY_IMAGE_NAME

# docker run --name $IMAGE_NAME --env-file .env.remote $IMAGE_NAME
