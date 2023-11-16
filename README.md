# avalanche-forecast

Personal project to use ML to forecast regional avalanche risks and avalanche problem types.

## Getting Started

Add the following to the end of your shell's `~/.profile` script.
Note that the `~/.profile` script name varies according to your shell. Run `echo $0` to get the shell you are using.

```sh
AVALANCHE_FORECAST_REPO=/path/to/this/repo

cd_avalanche_forecast() {
  cd $AVALANCHE_FORECAST_REPO
  . $AVALANCHE_FORECAST_REPO/terminal_aliases.sh
}
```

Close out of your current terminal tab and reopen it. You can now run `cd_avalanche_forecast` to cd into this
repo. This will give you access to all the utility terminal aliases in `terminal_aliases.sh`.
