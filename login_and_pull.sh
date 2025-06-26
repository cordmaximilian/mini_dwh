#!/bin/bash

# Script to attempt GitHub Container Registry login and pull required image.
# Failure to login or pull the image will not abort execution.

IMAGE="ghcr.io/duckdb/duckdb-wasm-shell:latest"

set +e  # Do not exit on errors so build continues

echo "Attempting to log in to GitHub Container Registry (ghcr.io)..."
docker login ghcr.io
LOGIN_STATUS=$?

if [ $LOGIN_STATUS -ne 0 ]; then
    echo "Login failed. Continuing without authentication."
fi

echo "Attempting to pull $IMAGE ..."
docker pull "$IMAGE"
PULL_STATUS=$?

if [ $PULL_STATUS -ne 0 ]; then
    echo "Failed to pull $IMAGE. Continuing with build."
fi

exit 0
