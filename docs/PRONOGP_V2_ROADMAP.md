# PronoGP V2 — roadmap de migration

## Portée actuelle : lot 1, déjà codé sur `prono-gp-v2`

- Correction des bonus en mode automatique / manuel valide / manuel refusé.
- Correction réversible, horodatée, sans modification des pronostics.
- Vérification des snapshots de réponses avant application de la dérogation.
- Calcul des points unifié (classement GP, classement général et détail par course).
- Navigation administration responsive.
- Tests unitaires de non-régression ; CI GitHub Actions.

## Restant à réaliser pour la V2 complète

### Lot 2 — Sécurité et gestion des saisons
- Sauvegarde de la DB PostgreSQL de production avec test de restauration.
- Audit des clés/identifiants de données de la saison 2026 et de leur persistance.
- Modèle multi-saisons 2026, 2027+ (migration additive) et classements isolés.
- Inscription / participation des joueurs par saison, archives lisibles.
- Test de migration avec copie anonymisée de la DB.

### Lot 3 — Expérience joueur inspirée de PronoSki
- Accueil avec sélecteur de saison et visuel MotoGP.
- Tableau de bord joueur : points, rang, dernier résultat, prochaine course.
- Podiums avec portraits ; gestion des photos de profil et stockage persistant.
- Recherche par numéro et par nom des pilotes ; gestion des absences.

### Lot 4 — Administration / Race Center
- Horaires par séance, édition administrative en heure Europe/Paris avec UTC en base.
- Fermeture des pronostics automatique côté serveur à la date/heure de clôture.
- Publication automatique des pronostics après clôture, désactivable.
- Modification administrative des horaires et historique de changement.
- Bloc Race Center : prochain GP, prochaine séance et compte à rebours.

### Lot 5 — PWA et notifications
- Notifications ciblées / collectives, catégories et historique en site.
- Rappels de clôture, calendrier et résultats.
- Planificateur gratuit sécurisé, sans garantie de précision à la minute.
- Installation web app sur smartphone.

## Prérequis de production

1. Sauvegarder la base de données, vérifier la restauration.
2. Déployer en environnement de préproduction distinct.
3. Vérifier comptes, pronostics réels et scores par GP.
4. Revalider les règles métier et confirmer les horaires de clôture.
5. Déployer avec stratégie de retour arrière.

Aucune migration complète de PronoSki ni aucun changement des barèmes MotoGP ne doivent être effectués implicitement.
