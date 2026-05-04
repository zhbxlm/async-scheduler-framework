#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="${IMAGE_NAME:-async-scheduler-framework}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
ACTION="${1:-build}"

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

print_tags() {
  echo "  ${IMAGE_NAME}:${IMAGE_TAG}"
  echo "  ${IMAGE_NAME}:api-${IMAGE_TAG}"
  echo "  ${IMAGE_NAME}:worker-${IMAGE_TAG}"
  echo "  ${IMAGE_NAME}:scheduler-${IMAGE_TAG}"
  echo "  ${IMAGE_NAME}:reconciler-${IMAGE_TAG}"
}

check_docker() {
  if command -v docker >/dev/null 2>&1; then
    echo "docker available"
    return 0
  fi
  echo "docker not available"
  return 1
}

case "$ACTION" in
  --check|check)
    check_docker
    ;;
  --dry-run|dry-run)
    echo "[dry-run] docker build -t ${IMAGE_NAME}:${IMAGE_TAG} ."
    echo "[dry-run] docker tag ${IMAGE_NAME}:${IMAGE_TAG} ${IMAGE_NAME}:api-${IMAGE_TAG}"
    echo "[dry-run] docker tag ${IMAGE_NAME}:${IMAGE_TAG} ${IMAGE_NAME}:worker-${IMAGE_TAG}"
    echo "[dry-run] docker tag ${IMAGE_NAME}:${IMAGE_TAG} ${IMAGE_NAME}:scheduler-${IMAGE_TAG}"
    echo "[dry-run] docker tag ${IMAGE_NAME}:${IMAGE_TAG} ${IMAGE_NAME}:reconciler-${IMAGE_TAG}"
    echo "Generated tags:"
    print_tags
    ;;
  build|"")
    check_docker
    echo "[build] docker image: ${IMAGE_NAME}:${IMAGE_TAG}"
    docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" .

    echo "[tag] service tags"
    docker tag "${IMAGE_NAME}:${IMAGE_TAG}" "${IMAGE_NAME}:api-${IMAGE_TAG}"
    docker tag "${IMAGE_NAME}:${IMAGE_TAG}" "${IMAGE_NAME}:worker-${IMAGE_TAG}"
    docker tag "${IMAGE_NAME}:${IMAGE_TAG}" "${IMAGE_NAME}:scheduler-${IMAGE_TAG}"
    docker tag "${IMAGE_NAME}:${IMAGE_TAG}" "${IMAGE_NAME}:reconciler-${IMAGE_TAG}"

    echo "Done. Generated tags:"
    print_tags
    ;;
  *)
    echo "Usage: $0 [build|--check|--dry-run]"
    exit 1
    ;;
esac
