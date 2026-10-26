# Déploiement contrôlé — PronoGP V2

**Statut : sauvegarde externe restaurée avec succès le 26/10/2026. Les tests CI PostgreSQL et métier sont au vert. Déploiement manuel contrôlé en préparation.**

Service de production connu : `motogp-jeu` sur Render, branche `main`, déploiement automatique **désactivé**. La branche de préparation est `prono-gp-v2` (PR GitHub #1).

## 1. Fonctions livrées sur la branche V2

- Multi-saisons, classement séparé et archive (2026 conservée) ;
- Espace personnel avec points, rang, dernier résultat, prochains pronostics ;
- Photos de profil (jusqu'à 10 Mo en entrée, compression JPEG, stockage PostgreSQL persistant) ;
- Gestion administrative des pilotes **par saison**, avec statut absent sans suppression de l'historique ;
- Race Center, séances et compte à rebours ;
- Clôture automatique côté serveur et publication automatique **optionnelle, cochée par défaut** à la première configuration ;
- Réajustement administrateur des dates / clôtures après échéance initiale ;
- Correction automatique + validation manuelle individuelle des bonus, traçable et réversible ;
- Onglets d'administration ;
- PWA installable, notifications dans le site, Web Push si VAPID configuré, messages administrateur ciblés ;
- Rappels de clôture et messages de résultats à traitement planifié par GitHub Actions.

**Important :** la notification dans le site n'implique pas l'envoi effectif d'un push sur téléphone. Le navigateur doit autoriser les notifications et les clés VAPID doivent être actives. La cadence gratuite GitHub Actions n'assure pas l'exécution à la minute exacte.

## 2. Conditions bloquantes avant publication

### A. Sauvegarde/restauration PostgreSQL

Sur un poste sécurisé équipé de `pg_dump` et `pg_restore`, avec `DATABASE_URL` renseignée **localement et non dans GitHub** :

```bash
bash scripts/backup_postgres.sh
```

La commande produit un dump custom, vérifie que sa liste est lisible et le protège par permission 600. Sauvegarder le fichier hors de Render.

Préparer une base de test **vide et distincte** puis renseigner `RESTORE_DATABASE_URL` sur ce poste et lancer :

```bash
bash scripts/test_restore_postgres.sh /chemin/pronogp-AAAAmmjjTHHMMSSZ.dump
```

L'essai doit restaurer `participants`, `pronos`, `pronos_history`, `results`, `weekends`, `championnat_pronos`, et les données originales. **Ne jamais restaurer dans la base de production.**

### B. Configuration Render

Vérifier la présence **sans exposer les valeurs** de :
- `DATABASE_URL` (PostgreSQL Supabase/production) ;
- `SECRET_KEY` unique, long, secret (NE PAS le changer inutilement : cela déconnecterait les joueurs) ;
- `ADMIN_USER`, `ADMIN_PASS` ;
- Pour les push téléphoniques : `VAPID_PUBLIC_KEY`, `VAPID_PRIVATE_KEY`, `VAPID_SUBJECT` (sujet de type `mailto:`).

**Ne pas inventer / remplacer ces secrets.** Les notifications enregistrées dans la base fonctionnent même sans VAPID, mais le push téléphonique reste désactivé.

### C. Configuration GitHub Actions

Le workflow `.github/workflows/push-hourly.yml` utilise par défaut l'URL HTTPS de production `https://motogp-jeu.onrender.com`. Une variable `PRONOGP_BASE_URL` peut la remplacer, sans être requise. Le workflow exige une preuve d'identité OIDC GitHub (pas de mot de passe partagé) et devient actif sur `main`. Il faut contrôler le dispatch et les clés VAPID pour la réception effective des push.

### D. Vérification fonctionnelle en environnement isolé

La CI a deux jobs : Python scoring et migration/tests PostgreSQL vierge. Cela ne remplace pas une copie restaurée de la base 2026.

Sur une copie isolée, tester :
1. Connexion d'un joueur historique et ses pronostics 2026 ;
2. Exactitude des points qualifs/sprint/GP/bonus, classement général et détail par GP ;
3. Une faute de frappe sur bonus : correction manuelle / retour à automatique ;
4. Clôture à une date passée : refus de nouvelle modification ; publication activée ou désactivée ; report dans le futur ;
5. Création d'une saison 2027 et copie des pilotes, absence/réactivation, sans changement des pronostics 2026 ;
6. Upload/suppression photo, podium et page personnelle sur mobile ;
7. Saisie calendrier, race center et passage été/hiver ;
8. Ciblage d'un message, boîte joueur, autorisation push et réception réelle après configuration VAPID.
9. Un GP avec ou sans Sprint, course annulée, questions bonus inconnues, absence de résultats.
10. Vérification qu'aucune modification de base ne supprime des enregistrements existants.

## 3. Publication

**Seulement après validation des points précédents :**

1. Noter le commit `main` et le dernier déploiement Render **live** (point de retour).
2. Mettre la PR #1 prête à relire, valider les deux checks CI, fusionner dans `main`.
3. Sur Render `motogp-jeu`, lancer **manuellement** un déploiement du dernier commit `main` (autoDeploy est désactivé). Attendre `live`.
4. Tester `/healthz` et `/health` puis connexion, classement, un GP public et l'espace joueur.
5. Surveiller les journaux Render/erreurs 5xx et les scores sur plusieurs GP historiques.
6. Vérifier le workflow de notifications après configuration de GitHub et VAPID.

## 4. Retour arrière

En cas de régression : arrêter toute modification admin, remettre le commit applicatif `main` précédent ou redéployer la version Render précédente **sans effacer ni restaurer les tables**. Les migrations V2 sont additives ; l'ancienne application ignore les nouvelles tables. Restaurer une sauvegarde complète seulement avec un plan précis, car remplacer une base effacerait les pronostics reçus depuis le dump.

## 5. Règles de fonctionnement

- Ne jamais annoncer un GP « public » si `is_pronos_public` est faux.
- Ne jamais rendre les pronostics publics avant la clôture prévue.
- Ne jamais détruire une saison déjà jouée.
- Pas de publication tant que la sauvegarde et sa restauration sur une copie n'ont pas été vérifiées.
- Le déploiement Render reste manuel.

## Contrôle de sauvegarde externe du 26/10/2026

Le propriétaire a exporté avec PostgreSQL 18 depuis **Supabase Session Pooler** et testé une restauration complète dans une **base PostgreSQL locale distincte** :

- Export : `PronoGP_20261026_221617.dump` (50 977 octets), conservé dans `Documents/PronoGP-Sauvegardes` sur le PC de l'utilisateur.
- Format : `pg_dump -Fc --schema=public`, fichier lisible avec `pg_restore --list`.
- Restauration : `pg_restore --single-transaction --exit-on-error` en base locale indépendante, terminée avec succès.
- Correspondance : **186** `pronos`, **349** `pronos_history`, **18** `results` dans la copie comme dans la base de production.
- Ne **jamais** ajouter ce dump à GitHub, Render ou à une pièce jointe non chiffrée. Les anciennes copies internes sont une sécurité supplémentaire, pas un substitut.

Le contrôle par l'utilisateur confirme le prérequis sauvegarde + restauration. Les autres vérifications fonctionnelles et le déploiement restent distincts.
