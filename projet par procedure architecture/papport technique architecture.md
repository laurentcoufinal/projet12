Rapport technique (version interne)
Vue d’ensemble de la plateforme DonnÉlite
1. Contexte technique 2
1.1. Historique technique de la plateforme 2
2. SI actuel 3
2.1. Architecture actuelle (vue simplifiée) 3
2.2. Stack technique actuelle 3
3. Issues clés 3
3.1. Dépendances identifiées 3
3.2. Contraintes techniques connues 3
3.3. Limites identifiées (non exhaustif) 4
3.4. Indicateurs observés ces 6 dernier mois 4
3.5. Impact écologique et technique (notes internes) 4
3.6. Notes complémentaires 4
1. Contexte technique
La plateforme DonnÉlite permet de collecter, traiter et analyser des flux de données issus de
différentes sources clients (API partenaires, fichiers batch, événements applicatifs).
Elle supporte actuellement :
● ingestion de données en temps réel (streaming)
● traitements batch quotidiens
● exposition de résultats via API et dashboards
La volumétrie a fortement augmenté ces derniers mois, avec :
● +300 % de données ingérées
● +150 % d’utilisateurs actifs
● augmentation significative des temps de traitement
1.1. Historique technique de la plateforme
La plateforme DonnÉlite a été initialement conçue comme un produit MVP destiné à quelques
clients pilotes du secteur retail.
L’architecture initiale reposait principalement sur :
● des traitements batch nocturnes ;
● une base PostgreSQL centralisée ;
● des APIs synchrones entre services.
Entre 2022 et 2024, plusieurs composants ont été ajoutés progressivement afin de répondre à la
croissance :
● introduction partielle de Kafka ;
● ajout du Data Lake ;
● création d’un service de scoring IA indépendant ;
● multiplication de microservices spécialisés.
Cependant, plusieurs migrations prévues n’ont jamais été finalisées :
● certains flux utilisent Kafka ;
● d’autres passent encore directement par API REST ;
● plusieurs composants critiques partagent toujours la même base analytique.
⚠️ Certaines décisions techniques historiques ne sont plus documentées.

image.png

2. SI actuel
2.1. Architecture actuelle (vue simplifiée)
Le système repose sur les composants principaux suivants :
● API Gateway
● Service d’ingestion temps réel
● Service de traitement batch (ETL)
● Message broker (Kafka)
● Data Lake (stockage brut)
● Base de données analytique (PostgreSQL)
● Service de scoring (modèles IA)
● Backend applicatif (API métier)
● Frontend dashboard
⚠️ Remarque : certains flux contournent Kafka pour des raisons historiques.
Le schéma ci-dessus représente une vue simplifiée du système. Certains flux et dépendances
historiques ne sont pas documentés ou ne sont connus que partiellement.
2.2. Stack technique actuelle
● Backend : Node.js / Python (hétérogène selon services)
● Data : Kafka, Spark (batch), PostgreSQL
● Stockage : S3-compatible
● Infra : Docker (non standardisé), déploiements manuels partiels
● Authentification : service interne
⚠️ Plusieurs versions de librairies coexistent.
3. Issues clés
3.1. Dépendances identifiées
● Forte dépendance entre ingestion et traitement batch
● Couplage important entre backend applicatif et base analytique
● Service de scoring dépend directement du Data Lake
⚠️ Les dépendances ne sont pas toutes documentées.
3.2. Contraintes techniques connues
● Difficulté à scaler certains traitements batch
● Latence élevée sur certaines API
● Problèmes de synchronisation entre données temps réel et batch
● Manque d’isolation entre services
3.3. Limites identifiées (non exhaustif)
● Architecture hybride peu maîtrisée (stream + batch)
● Redondance de certaines données
● Difficulté à intégrer de nouveaux composants
● Faible standardisation des pratiques
3.4. Indicateurs observés ces 6 dernier mois
Indicateur Valeur
observée
Volume quotidien de données ingérées ~18 To/jour
Croissance mensuelle moyenne +12 %
Temps moyen du pipeline batch nocturne 9h15
Temps moyen de réponse API aux pics 4 à 7 secondes
Nombre moyen d’incidents critiques/mois 6
Volume estimé de données dupliquées ~30 %
Temps moyen de reconstruction d’un pipeline en
incident
3h
Taux estimé de services monitorés correctement ~60 %
⚠️ Certaines métriques sont estimées et non consolidées.
3.5. Impact écologique et technique (notes internes)
● Stockage de données peu optimisé (duplication fréquente)
● Jobs batch gourmands en ressources
● Absence de stratégie de rétention claire
3.6. Notes complémentaires
● Une migration vers une architecture plus orientée événements a été évoquée mais non
engagée
● Certains services critiques n’ont pas de monitoring fiable