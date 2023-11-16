# Terminal aliases to improve development iteration.
#
# See the "Getting Started" section of this repo's README to configure these aliases.


IMAGE_NAME="avalanche-forecast"

build_image() {
  # Build the application docker image
  docker build . -t $IMAGE_NAME
}

start_webserver() {
  # Start an application webserver using the provided app and port. Defaults to port 8080 if no port is provided.
  # Example usage: start_webserver path.to.app:app_name 8181
  app=$1
  port=$2
  docker run --rm -it -p ${port:-8080}:8080 $IMAGE_NAME $app
}

start_local() {
  # Start a container with bash and mount the application code as a volume to /sandbox
  docker run --rm -it -v $AVALANCHE_FORECAST_REPO:/sandbox -w /sandbox --entrypoint sh $IMAGE_NAME
}
