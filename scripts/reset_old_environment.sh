#!/usr/bin/env bash
# One-time cleanup of containers from the OLD version of this project.
#
# Those containers have fixed names (kafka, postgres, ...) and hold the ports
# 8080, 8085, 8888, 9000, 9001, 9094 and 5432, so the new stack can't start until they're gone.
# This removes containers only. It does NOT delete images or your project files.
#
# Run from anywhere:  bash scripts/reset_old_environment.sh

OLD=(kafka kafka-ui minio postgres airflow-init airflow-webserver airflow-scheduler workspace)

echo "This will force-remove these containers if they exist:"
printf '  %s\n' "${OLD[@]}"
echo
read -r -p "Continue? (y/N) " answer
if [[ "$answer" != "y" && "$answer" != "Y" ]]; then
  echo "Cancelled. Nothing was changed."
  exit 0
fi

for c in "${OLD[@]}"; do
  docker rm -f "$c" >/dev/null 2>&1 && echo "  removed $c" || echo "  (no container named $c)"
done

echo
echo "Done. Next: from the project folder run   docker compose up -d --build"
