# run_docker_bash.sh
#
# Builds and runs a Docker container and then enables
# an interactive bash terminal from which users can view
# container contents and execute Python scripts. Mounts
# the entire pipeline directory to permit testing without
# container restarts.
# 

IMAGE_NAME=bog-pipeline
CONTAINER_NAME=bog-pipeline

docker build -t $IMAGE_NAME .
docker run --name $CONTAINER_NAME \
    -v "/${PWD}:/bog" \
    -it --rm $IMAGE_NAME bash