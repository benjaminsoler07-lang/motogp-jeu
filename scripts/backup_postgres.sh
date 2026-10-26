#!/usr/bin/env bash
# Sauvegarde avant mise à jour PronoGP V2. Ne contient aucune information d'accès.
set -euo pipefail
umask 077
if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "Erreur : variable DATABASE_URL absente." >&2
  exit 1
fi
if ! command -v pg_dump >/dev/null 2>&1 || ! command -v pg_restore >/dev/null 2>&1; then
  echo "PostgreSQL client requis : pg_dump et pg_restore." >&2
  exit 1
fi
backup_dir="${BACKUP_DIR:-${HOME}/pronogp-backups}"
mkdir -p "$backup_dir"
chmod 700 "$backup_dir"
output="$backup_dir/pronogp-$(date -u +%Y%m%dT%H%M%SZ).dump"
pg_dump --format=custom --no-owner --no-acl --file="$output" "$DATABASE_URL"
pg_restore --list "$output" >/dev/null
chmod 600 "$output"
echo "Sauvegarde vérifiée (catalogue PostgreSQL lisible) : $output"
echo "IMPORTANT : effectuer aussi un essai de restauration sur une base DISTINCTE avant production."
