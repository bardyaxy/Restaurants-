#!/usr/bin/env bash
set -e
# Run the refresh-restaurants entry point. If the package is installed, you can
# use `refresh-restaurants` directly. Running via `python -m` ensures it works
# from a source checkout as well.
python -m restaurants.refresh_restaurants "$@"
