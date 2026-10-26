#!/usr/bin/env bash
# Essai de restauration UNIQUEMENT vers une base de test distincte.
set -euo pipefail
umask 077
if [[ -z "${DATABASE_URL:-}" || -z "${RESTORE_DATABASE_URL:-}" || $# -ne 1 ]]; then
  echo "Utilisation : DATABASE_URL=... RESTORE_DATABASE_URL=... $0 /chemin/backup.dump" >&2
  exit 1
fi
if [[ "$DATABASE_URL" == "$RESTORE_DATABASE_URL" ]]; then
  echo "Refus : les URL de production et de restauration sont identiques." >&2
  exit 1
fi
echo "ATTENTION : base cible de test uniquement (non production)." >&2
pg_restore --list "$1" >/dev/null
# Exige une base de test VIDE, et ne détruit aucun objet existant.
pg_restore --no-owner --no-acl --single-transaction --exit-on-error \
  --dbname="$RESTORE_DATABASE_URL" "$1"
echo "Essai de restauration terminé. Contrôler les tables pronos/pronos_history/results."
