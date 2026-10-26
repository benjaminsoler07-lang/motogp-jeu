# PronoGP V2 — vérification avant production (26/10/2026)

## État validé

- Render : service `motogp-jeu`, branche `main`, autoDeploy désactivé ; ancienne version `live`.
- GitHub : branche `prono-gp-v2`, PR #1, deux jobs CI (tests métier et PostgreSQL) au vert au dernier commit vérifié.
- Supabase : projet `PronoGP` (ref `btgzozoigsebxkuvjlhb`), actif, organisation sur plan **free**.
- Comptages exacts observés sans écriture : 13 participants ; 186 pronostics ; 349 éléments d'historique ; 18 résultats ; 22 GP ; 11 pronostics championnat.
- Aucune migration V2 déclenchée sur la production ; aucune fusion `main` ; aucun déploiement Render V2.

## BLOQUANT 1 — Sauvegarde indépendante

Le plan Supabase Free ne garantit pas la disponibilité d'une sauvegarde quotidienne téléchargeable.
Documentation : https://supabase.com/docs/guides/platform/backups

Il faut exporter la base *hors Supabase*, avec `pg_dump` ou `supabase db dump`, puis restaurer ce dump dans une base de test distincte et comparer les comptes ci-dessus. Les scripts `scripts/backup_postgres.sh` et `scripts/test_restore_postgres.sh` sont prêts mais ne sont PAS exécutés. Ne jamais placer un dump contenant des secrets/profils de joueurs dans ce dépôt GitHub.

## BLOQUANT 2 — Sécurité des tables publiques

Le contrôle Supabase du 26/10/2026 confirme que les tables suivantes ont `relrowsecurity=false` et que le rôle `anon` a les droits `SELECT` et `INSERT` :
`participants`, `pronos`, `pronos_history`, `results`, `weekends`, `championnat_pronos`, `championnat_state`, `push_subscriptions`.

Cela peut exposer et laisser modifier les données directement par l'API Supabase. La nouvelle V2 ajoute en plus d'autres données sensibles (photos, abonnements, messages). **Ne pas activer la V2 avant une revue des privilèges.**

Approche à étudier : si Flask est le seul accès à la base, retirer l'accès direct `anon` et `authenticated` aux tables, ou activer RLS et mettre en place les politiques adaptées. Ce point exige d'abord de vérifier l'identité du rôle utilisé par l'application et les usages éventuels de l'API Supabase. **Aucune permission ni RLS n'a été modifiée automatiquement.**

Docs : https://supabase.com/docs/guides/database/postgres/row-level-security

## Étapes pour débloquer

1. Produire une sauvegarde hors site, vérifier sa restauration sur une base indépendante et noter un checksum.
2. Définir/tester les permissions Supabase et RLS adaptées à l'accès Flask ; confirmer qu'aucun client navigateur ne nécessite ces droits.
3. Déployer la V2 sur une préproduction connectée à la base restaurée et comparer les scores, pronostics et classements 2026.
4. Fusionner la PR #1 uniquement après les tests ; déclencher manuellement Render ; contrôler `/health`, authentification et résultats.

**Il est interdit de déclarer la V2 « en ligne » sans confirmation du déploiement Render live.**
