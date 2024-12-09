# Project terminal aliases. See the root README for notes on configuring.

set_env() {
  # Options include "dev" or "prod"
  env=$1
  rm ./docker-compose.override.yaml
  ln -s ./docker-compose.$env.yaml ./docker-compose.override.yaml
}

start_sandbox() {
  # Start a container with bash and mount the application code as a volume to /sandbox
  docker run \
    --rm \
    -it \
    -e PYTHONPATH=/sandbox:/ \
    -v $AVALANCHE_FORECAST_REPO:/sandbox \
    -w /sandbox \
    --entrypoint /bin/bash \
    avalanche-forecast-dagster-webserver
}
