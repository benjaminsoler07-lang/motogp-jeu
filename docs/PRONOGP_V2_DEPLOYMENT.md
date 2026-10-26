# PronoGP V2 — déploiement sécurisé

## Statut : branche validée sur base PostgreSQL de test, non publiée

Le code est sur `prono-gp-v2`, PR #1 en **brouillon**. Ne pas merger pendant une session de pronostics sans sauvegarde.

### Fonctionnalités intégrées
- Espace personnel joueur et bandeau Race Center.
- Programme des séances et date de clôture en heure de Paris, stockés en UTC.
- Fermeture automatique côté serveur et publication optionnelle après clôture.
- Sélecteur de pilotes par recherche.
- Comptes sécurisés par session authentifiée : **les anciens utilisateurs doivent se reconnecter après le déploiement**.
- Photos de profil persistantes en PostgreSQL, visibles sur les podiums.
- Création de saisons/GP, archives et championnat isolé par saison.
- Validation manuelle individuelle des réponses bonus et recalcul centralisé.
- Historique et abonnements notifications Web Push ; ciblage administratif.
- Planificateur gratuit GitHub Actions (OIDC, fréquence horaire).

### Variables Render requises
- `DATABASE_URL` : PostgreSQL persistant existant.
- `SECRET_KEY` : clé longue, aléatoire et **stable** pour les sessions.
- `ADMIN_USER`, `ADMIN_PASS` : conserver les identifiants administrateur.
- `VAPID_PUBLIC_KEY`, `VAPID_PRIVATE_KEY`, `VAPID_SUBJECT` : clés Web Push du service PronoGP.

Ne jamais commiter de clés ni de mots de passe. Sans clés VAPID, le site fonctionne mais les push téléphone sont indisponibles.

### Variable GitHub Actions
- `PRONOGP_BASE_URL` : l'URL HTTPS du service Render **réel**, sans slash final.

Le workflow `.github/workflows/push-hourly.yml` s'active sur la branche par défaut après la fusion ; ses alertes ne sont pas garanties à la minute (GitHub Actions/Render gratuits).

### Sauvegarde et retour arrière
1. Exporter intégralement la base PostgreSQL (tables participants, pronos, pronos_history, results, weekends, championnat_pronos, championnat_state, bonus_reviews).
2. Vérifier sur une **base de test restaurée** que comptes, pronostics et scores 2026 sont inchangés.
3. Vérifier les variables Render et l'URL GitHub Actions.
4. Tester l'authentification de nouveaux et anciens comptes, les uploads photos et les scores.
5. Programmer le passage sur main hors période critique.
6. Conserver l'ancienne image/commit et une sauvegarde DB pour un retour arrière.

Les migrations dans `db_init` sont additives. Le retour arrière du code est simple ; ne pas supprimer les nouvelles tables si elles contiennent déjà des données. Une sauvegarde DB est toujours requise.

### Vérifications CI
`.github/workflows/pronogp-v2-tests.yml` exécute :
- Vérification de la syntaxe Python et tests du barème.
- Tests des pages, fuseaux horaires et contrôle des bonus.
- Démarrage avec PostgreSQL 16 vierge, migrations, connexion par session, photo, notifications et saisons.

La CI ne remplace pas un test de restauration de la base de production ; les données existantes et leurs contraintes peuvent être différentes.

### Risques à vérifier avant passage en production
- Nouvelle authentification par session : reconnexion des joueurs obligatoire.
- Gestion des GP anciens et des identifiants 2027 : confirmer toutes les références.
- Enregistrement de séances en heure de Paris : vérification le jour d'un changement d'heure.
- Push dépend des clés VAPID et des abonnements validés sur chaque appareil.
- Ne jamais démarrer le GP en course avec une copie de base non vérifiée.
