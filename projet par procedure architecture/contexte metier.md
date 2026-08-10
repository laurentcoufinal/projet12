Document interne
Présentation métier et évolution fonctionnelle de la plateforme DonnÉlite
1. Présentation de l’entreprise 2
2. Périmètre fonctionnel actuel 2
a. Collecte de données 2
b. Consolidation et traitement des données 2
c. Consultation des indicateurs 3
d. Consommation des résultats via API 3
e. Scoring prédictif 3
3. Limites fonctionnelles identifiées 3
a. Temps de disponibilité des données 3
b. Difficulté d’intégration de nouvelles sources 4
c. Difficulté d’intégration de nouveaux modèles IA 4
d. Expérience des clients Enterprise 4
4. Nouveau périmètre fonctionnel attendu 4
a. Cas d’usage 1 — Analyse quasi temps réel 4
b. Cas d’usage 2 — Intégration simplifiée de nouvelles sources 4
c. Cas d’usage 3 — Déploiement plus fréquent de modèles IA 5
d. Cas d’usage 4 — Plateforme orientée clients Enterprise 5
e. Cas d’usage 5 — Self-service analytics 5
5. Contraintes métier à prendre en compte 5
a. Contraintes économiques 6
b. Contraintes opérationnelles 6
c. Contraintes organisationnelles 6
d. Contraintes de durabilité 6
1. Présentation de l’entreprise
DonnÉlite est une scale-up spécialisée dans l’analyse de données opérationnelles en temps réel.
Nos clients sont principalement des entreprises des secteurs :
● retail ;
● logistique ;
● transport ;
● finance.
Notre plateforme permet à ces organisations de centraliser des données provenant de multiples sources, puis de les exploiter afin de prendre des décisions opérationnelles plus rapidement.
Les principaux utilisateurs de notre plateforme sont :
● des analystes métier ;
● des responsables opérationnels ;
● des data scientists ;
● des applications tierces utilisant les APIs analytiques.
2. Périmètre fonctionnel actuel
Notre plateforme actuelle permet de couvrir les cas d’usage suivants :
a. Collecte de données
Les clients peuvent connecter plusieurs sources de données :
● APIs partenaires ;
● exports CSV ;
● applications métiers ;
● événements applicatifs.
Les données sont collectées puis stockées dans le système de DonnÉlite.
b. Consolidation et traitement des données
Les données collectées sont :
● nettoyées ;
● enrichies ;
● agrégées.
Une partie importante des traitements est réalisée par des pipelines batch exécutés quotidiennement.
c. Consultation des indicateurs
Les utilisateurs peuvent consulter :
● des tableaux de bord ;
● des indicateurs métiers ;
● des rapports analytiques.
Les données affichées peuvent avoir plusieurs heures de décalage avec la réalité opérationnelle.
d. Consommation des résultats via API
Les clients peuvent récupérer certains indicateurs via des APIs.
Ces APIs sont principalement utilisées pour :
● l’intégration dans des outils tiers ;
● l’alimentation de rapports internes ;
● l’automatisation de processus métier.
e. Scoring prédictif
La plateforme fournit quelques modèles prédictifs préconfigurés.
Exemples :
● prévision de volumes logistiques ;
● estimation de risques ;
● détection simple d’anomalies.
L’intégration de nouveaux modèles reste complexe et nécessite souvent une intervention de l’équipe Engineering.
3. Limites fonctionnelles identifiées
Les équipes Produit et Customer Success ont identifié plusieurs limites récurrentes.
a. Temps de disponibilité des données
Les clients souhaitent disposer d’informations plus fraîches. Aujourd’hui :
● certains indicateurs sont mis à jour plusieurs heures après les événements ;
● les analyses quasi temps réel restent limitées.
b. Difficulté d’intégration de nouvelles sources
L’ajout d’une nouvelle source de données nécessite souvent :
● des développements spécifiques ;
● des adaptations manuelles des pipelines existants.
Le délai moyen d’intégration est jugé trop important.
c. Difficulté d’intégration de nouveaux modèles IA
Les équipes Data Science rencontrent régulièrement des difficultés pour :
● déployer un nouveau modèle ;
● faire évoluer un modèle existant ;
● réutiliser les données produites par d’autres équipes.
d. Expérience des clients Enterprise
Les clients Enterprise expriment plusieurs attentes :
● meilleure disponibilité des APIs ;
● meilleure traçabilité des traitements ;
● garanties renforcées sur la qualité des données.
4. Nouveau périmètre fonctionnel attendu
Dans le cadre du plan stratégique chez DonnÉlite, nous souhaitons faire évoluer notre plateforme.
Les besoins ci-dessous sont considérés comme prioritaires.
a. Cas d’usage 1 — Analyse quasi temps réel
Les utilisateurs doivent pouvoir :
● visualiser certains indicateurs quelques minutes après réception des données ;
● être alertés rapidement en cas d’événement critique ;
● limiter la dépendance aux traitements batch nocturnes.
b. Cas d’usage 2 — Intégration simplifiée de nouvelles sources
Les équipes souhaitent réduire le temps nécessaire à l’intégration d’une nouvelle source de données.
Objectif :
● permettre l’ajout de nouvelles sources sans modification majeure de l’architecture.
c. Cas d’usage 3 — Déploiement plus fréquent de modèles IA
Les Data Scientists doivent pouvoir :
● déployer plus facilement de nouveaux modèles ;
● tester plusieurs versions d’un modèle ;
● réutiliser les données produites par différents composants de la plateforme.
d. Cas d’usage 4 — Plateforme orientée clients Enterprise
Les clients Enterprise demandent :
● des engagements de disponibilité plus élevés ;
● des temps de réponse plus stables ;
● une meilleure isolation de leurs traitements ;
● davantage de visibilité sur les données traitées.
e. Cas d’usage 5 — Self-service analytics
Les équipes métier souhaitent pouvoir :
● créer certains indicateurs sans intervention systématique des équipes techniques ;
● explorer plus facilement les données disponibles ;
● construire des tableaux de bord personnalisés.
5. Contraintes métier à prendre en compte
L’évolution du système devra également respecter plusieurs contraintes :
a. Contraintes économiques
● La croissance des coûts cloud doit rester maîtrisée.
● Les solutions retenues doivent être soutenables à moyen terme.
b. Contraintes opérationnelles
● La plateforme doit continuer à fonctionner pendant la transition.
● Les composants existants ne peuvent pas tous être remplacés immédiatement.
c. Contraintes organisationnelles
● Les équipes Engineering, Data et Produit doivent pouvoir travailler de manière plus autonome.
● Les nouveaux composants doivent être intégrables progressivement.
d. Contraintes de durabilité
● La réduction des duplications de données est un objectif stratégique.
● Les impacts écologiques des choix techniques doivent être pris en compte lors des arbitrages architecturaux.