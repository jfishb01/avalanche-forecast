# Project terminal aliases. See the root README for notes on configuring.

set_env() {
  # Options include "dev" or "prod"
  env=$1
  rm ./docker-compose.override.yaml
  ln -s ./docker-compose.$env.yaml ./docker-compose.override.yaml
}


WEBSERVER_IMAGE_NAME="avalanche-forecast-webserver"
DB_IMAGE_NAME_BASE="avalanche-forecast-db"
DB_IMAGE_NAME_PERSISTENT="$DB_IMAGE_NAME_BASE-persistent"
DB_IMAGE_NAME_SANDBOX="$DB_IMAGE_NAME_BASE-sandbox"
MLFLOW_IMAGE_NAME_BASE="avalanche-forecast-mlflow"
MLFLOW_IMAGE_NAME_PERSISTENT="$MLFLOW_IMAGE_NAME_BASE-persistent"
MLFLOW_IMAGE_NAME_SANDBOX="$MLFLOW_IMAGE_NAME_BASE-sandbox"


build_image() {
  # Build the application docker image
  docker build . -t $WEBSERVER_IMAGE_NAME
}

start_webserver() {
  # Start an application webserver using the provided app and port. Defaults to port 8080 if no port is provided.
  # Example usage: start_webserver path.to.app:app_name 8181
  app=$1
  port=$2
  docker run --rm -it -p ${port:-8080}:3000 $WEBSERVER_IMAGE_NAME $app
}

start_local() {
  # Start a container with bash and mount the application code as a volume to /sandbox
  docker run \
    --rm \
    -it \
    -e PYTHONPATH=/sandbox:/ \
    -v $AVALANCHE_FORECAST_REPO:/sandbox \
    -w /sandbox \
    --entrypoint /bin/bash \
    $WEBSERVER_IMAGE_NAME
}

start_db() {
  # Start a clickhouse DB server to host application data. Providing the "--sandbox" flag will use a fresh DB
  # instance and will not persist any of the data.
  volume_mount_base_dir="$AVALANCHE_FORECAST_REPO/resources/clickhouse"

  image_name=$DB_IMAGE_NAME_PERSISTENT
  persistent_data_volume_mount="-v $volume_mount_base_dir/data:/var/lib/clickhouse/ "
  while [ $# -gt 0 ]; do
    case $1 in
      --sandbox)
        persistent_data_volume_mount=""
        image_name=$DB_IMAGE_NAME_SANDBOX
    esac
    shift
  done

  rm $AVALANCHE_FORECAST_REPO/resources/clickhouse/logs/*.log &> /dev/null
  docker run \
    -d \
    -p 18123:8123 \
    -p 19000:9000 \
    -v $AVALANCHE_FORECAST_REPO/downloads:/sandbox/downloads \
    -v $volume_mount_base_dir/config:/etc/clickhouse-server \
    -v $volume_mount_base_dir/logs:/var/log/clickhouse-server \
    -v $volume_mount_base_dir/init:/docker-entrypoint-initdb.d \
    $persistent_data_volume_mount \
    --name $image_name \
    --ulimit nofile=262144:262144 \
    clickhouse/clickhouse-server:22-alpine
}

stop_db() {
  # Stop any existing local clickhouse DB instances
  _stop_resource $DB_IMAGE_NAME_PERSISTENT $DB_IMAGE_NAME_SANDBOX
}


start_mlflow() {
  # Start an mlflow server to host application data. Providing the "--sandbox" flag will use a fresh mlflow
  # instance and will not persist any of the data.
  volume_mount_base_dir="$AVALANCHE_FORECAST_REPO/resources/mlflow"
  backend_store_uri="/mlflow/data"

  image_name=$DB_IMAGE_NAME_PERSISTENT
  persistent_data_volume_mount="-v $volume_mount_base_dir:$backend_store_uri "
  while [ $# -gt 0 ]; do
    case $1 in
      --sandbox)
        persistent_data_volume_mount=""
        image_name=$MLFLOW_IMAGE_NAME_SANDBOX
    esac
    shift
  done

  port=5000
  docker run \
    --rm \
    -d \
    -p $port:$port \
     \
    $persistent_data_volume_mount \
    --name $image_name \
    ghcr.io/mlflow/mlflow:v2.9.2 \
    /bin/bash -c "mlflow server --host 0.0.0.0 --port $port --backend-store-uri $backend_store_uri"
}


stop_mlflow() {
  # Stop any existing local mlflow server instances
  _stop_resource $MLFLOW_IMAGE_NAME_PERSISTENT $MLFLOW_IMAGE_NAME_SANDBOX
}

_stop_resource() {
  persistent_resource_name=$1
  sandbox_resource_name=$2
  persistent_resource=$(docker container ls -a | grep $persistent_resource_name | awk '{ print $1 }')
  sandbox_resource=$(docker container ls -a | grep $sandbox_resource_name | awk '{ print $1 }')
  if [ -n "$persistent_resource" ]; then
    docker container stop $persistent_resource_name > /dev/null
    docker rm $persistent_resource_name > /dev/null
  fi
  if [ -n "$sandbox_resource" ]; then
    docker container stop $sandbox_resource_name > /dev/null
    docker rm $sandbox_resource_name > /dev/null
  fi
}
