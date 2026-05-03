#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="${IMAGE_NAME:-async-scheduler-framework}"
IMAGE_TAG="${IMAGE_TAG:-latest}"

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

echo "[build] docker image: ${IMAGE_NAME}:${IMAGE_TAG}"
docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" .

echo "[tag] service tags"
docker tag "${IMAGE_NAME}:${IMAGE_TAG}" "${IMAGE_NAME}:api-${IMAGE_TAG}"
docker tag "${IMAGE_NAME}:${IMAGE_TAG}" "${IMAGE_NAME}:worker-${IMAGE_TAG}"
docker tag "${IMAGE_NAME}:${IMAGE_TAG}" "${IMAGE_NAME}:scheduler-${IMAGE_TAG}"
docker tag "${IMAGE_NAME}:${IMAGE_TAG}" "${IMAGE_NAME}:reconciler-${IMAGE_TAG}"

echo "Done. Generated tags:"
echo "  ${IMAGE_NAME}:${IMAGE_TAG}"
echo "  ${IMAGE_NAME}:api-${IMAGE_TAG}"
echo "  ${IMAGE_NAME}:worker-${IMAGE_TAG}"
echo "  ${IMAGE_NAME}:scheduler-${IMAGE_TAG}"
echo "  ${IMAGE_NAME}:reconciler-${IMAGE_TAG}"
