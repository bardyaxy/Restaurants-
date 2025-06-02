#!/usr/bin/env bash
set -e
python refresh_restaurants.py
latest=$(ls -t *_google_restaurants_*.csv | head -1)
python loader.py "$latest"
