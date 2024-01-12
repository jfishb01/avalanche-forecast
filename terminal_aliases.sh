# Terminal aliases to improve development iteration.
#
# See the "Getting Started" section of this repo's README to configure these aliases.


WEBSERVER_IMAGE_NAME="avalanche-forecast-webserver"
DB_IMAGE_NAME_BASE="avalanche-forecast-db"
DB_IMAGE_NAME_PERSISTENT="$DB_IMAGE_NAME_BASE-persistent"
DB_IMAGE_NAME_SANDBOX="$DB_IMAGE_NAME_BASE-sandbox"


build_image() {
  # Build the application docker image
  docker build . -t $WEBSERVER_IMAGE_NAME
}

start_webserver() {
  # Start an application webserver using the provided app and port. Defaults to port 8080 if no port is provided.
  # Example usage: start_webserver path.to.app:app_name 8181
  app=$1
  port=$2
  docker run --rm -it -p ${port:-8080}:8080 $WEBSERVER_IMAGE_NAME $app
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
  # Stop any existing clickhouse DB instances
  persistend_db=$(docker container ls | grep $DB_IMAGE_NAME_PERSISTENT | awk '{ print $1 }')
  sandbox_db=$(docker container ls | grep $DB_IMAGE_NAME_SANDBOX | awk '{ print $1 }')
  if [ -n "$persistend_db" ]; then
    docker container stop $DB_IMAGE_NAME_PERSISTENT > /dev/null
    docker rm $DB_IMAGE_NAME_PERSISTENT > /dev/null
  fi
  if [ -n "$sandbox_db" ]; then
    docker container stop $DB_IMAGE_NAME_SANDBOX > /dev/null
    docker rm $DB_IMAGE_NAME_SANDBOX > /dev/null
  fi
}
