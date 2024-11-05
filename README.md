# avalanche-forecast

Personal project using ML to forecast regional avalanche risks and avalanche problem types.

## Installation

Install [docker compose](https://docs.docker.com/compose/install/)

Navigate to the repo home directory and build the docker images:
```shell
docker compose build
```

## Execution

Run in production (data will be persisted):
```shell
docker compose up -f docker-compose.yaml -f docker-compose.prod.yaml
```

Run in development (data will be transient, live updates to code will be reflected during execution):
```shell
docker compose up -f docker-compose.yaml -f docker-compose.dev.yaml
```

## Convenience Utilities

A number of terminal aliases have been added for convenience to [terminal_aliases.sh](https://github.com/jfishb01/avalanche-forecast/blob/main/terminal_aliases.sh).

To have these aliases loaded when navigating to this project, add the following to the
end of your shell's profile script:
```shell
AVALANCHE_FORECAST_REPO=/path/to/this/repo

cd_avalanche_forecast() {
  cd $AVALANCHE_FORECAST_REPO
  . $AVALANCHE_FORECAST_REPO/terminal_aliases.sh
}
```

Close out of your current terminal tab and reopen it. You can now run `cd_avalanche_forecast` to
cd into this repo and load all the project terminal aliases.
