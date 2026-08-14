#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "Babel Gateway strict demonstration"

if ! docker image inspect babel-go-service-a:local >/dev/null 2>&1; then
  echo "Loading supplied backend images..."
  docker load -i images/babel-go-images.tar
fi

docker compose up -d --build

echo "Waiting for the gateway..."
for attempt in $(seq 1 60); do
  if curl --fail --silent http://localhost:8080/status >/dev/null; then
    break
  fi
  if [ "$attempt" -eq 60 ]; then
    echo "Gateway did not become ready."
    docker compose logs gateway
    exit 1
  fi
  sleep 1
done

PYTHONPATH=. python3 demo/test_scenarios.py
