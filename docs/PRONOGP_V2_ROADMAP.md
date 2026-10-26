# PronoGP V2 — suivi de la préparation

## Réalisé sur `prono-gp-v2` (non publié tant que la migration réelle n'est pas validée)

- ✅ Validation manuelle/automatique des bonus, traçable, réversible, par joueur et question ;
- ✅ Barème préservé et calcul harmonisé dans les trois affichages des scores ;
- ✅ Onglets d'administration responsive ;
- ✅ Multi-saisons, championnat distinct par saison et conservation des données 2026 ;
- ✅ Administration pilotes et équipes officielles/satellites par saison (activation/désactivation) ;
- ✅ Tableau de bord joueur, podiums avec avatars et photos de profil persistantes ;
- ✅ Race Center avec horaires, sessions et compte à rebours ;
- ✅ Clôture côté serveur, publication automatique en option (cochée initialement) ;
- ✅ Modification admin des dates et horaires après expiration du créneau initial ;
- ✅ PWA, notifications internes, notifications push sous réserve de VAPID, ciblage joueur et historique administrateur ;
- ✅ Rappels automatiques et notifications de résultats après clôture, déclenchés par GitHub Actions avec OIDC ;
- ✅ Tests unitaires + intégration sur PostgreSQL GitHub Actions ;
- ✅ Scripts de sauvegarde et essai de restauration sur une base distincte ;
- ✅ Guide de déploiement/retour arrière dans `docs/MISE_EN_PRODUCTION_V2.md`.

## Vérifications extérieures au dépôt encore requises avant une publication

1. Sauvegarde réelle PostgreSQL de production et restauration validée dans une base de test ;
2. Vérification non intrusive des accès/secrets Render (`DATABASE_URL`, `SECRET_KEY`, `ADMIN_USER`, `ADMIN_PASS`) ;
3. Configuration VAPID sur Render pour recevoir des push sur les téléphones ;
4. Configuration de la variable GitHub Actions `PRONOGP_BASE_URL` ;
5. Validation fonctionnelle complète avec copie de données historiques 2026, scores de référence et tests mobiles ;
6. Fusion de la PR et déploiement **manuel** sur Render, avec point de retour et contrôle `/health`.

## Limites explicites

- Les horaires de séances et la clôture doivent être saisis GP par GP côté administration : aucune source MotoGP externe n'est branchée.
- Les notifications GitHub Actions sont gratuites mais non garanties à la minute.
- Les notifications sont consultables dans le site sans VAPID ; la réception sur le téléphone exige VAPID + consentement du navigateur.
- La sécurité et la récupération des données nécessitent une vraie sauvegarde distincte ; les tests PostgreSQL CI ne sont pas une sauvegarde de production.

**Ne pas annoncer « en ligne » avant réussite de ces contrôles.**
