#!/usr/bin/env bash
set -e
# Install runtime and development dependencies for running tests
pip install -e .
pip install -r requirements-dev.txt
pytest -q
