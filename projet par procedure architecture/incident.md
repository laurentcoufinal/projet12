📂 Dossier interne (mélange de sources)
📩 Emails..........................................................................................................................................2
📩 Direction produit......................................................................................................................2
📩 Client.......................................................................................................................................2
📩 Direction financière.................................................................................................................2
📩 Client Enterprise.....................................................................................................................2
💬 Messages.....................................................................................................................................2
💬 Équipe Data Analysis..............................................................................................................2
💬 Équipe Produit........................................................................................................................3
💬 Équipe Data Engineering........................................................................................................3
⚠️
Incidents......................................................................................................................................3
⚠️
Incident #245 – Rapport simplifié – Latence API....................................................................3
⚠️
Incident #312 – Rapport simplifié – Perte de données batch.................................................3
⚠️
Extrait réunion incident...........................................................................................................4
📝 Notes internes diverses.............................................................................................................4
📝 Équipe Engineering................................................................................................................4
📝 SRE.........................................................................................................................................4
📊 Feedback interne – Data Scientist..........................................................................................4
📊 Note produit – Objectifs 2026.................................................................................................4
📉 Note écologique (extrait).........................................................................................................5
🧩 Hypothèses évoquées (non validées).....................................................................................5
📩 Emails
📩 Direction produit
“Nous avons de plus en plus de clients qui demandent des analyses quasi temps réel. Aujourd’hui, les délais sont trop longs et certains dashboards sont obsolètes.”
📩 Client
“Nous constatons des incohérences entre les données affichées dans votre dashboard et celles reçues via API.”
📩 Direction financière
“Attention à la hausse continue des coûts cloud. Toute nouvelle architecture devra limiter l’augmentation des coûts d’infrastructure.”
📩 Client Enterprise
“Notre contrat prévoit un SLA de 99,9 % sur les APIs analytiques. Les incidents du mois dernier ont déjà impacté nos opérations logistiques.”
💬 Messages
💬 Équipe Data Analysis
“Le job batch de nuit a encore échoué hier. On a dû relancer manuellement. On ne comprend pas encore pourquoi ça ne tient pas la charge.”
💬 Équipe Produit
“Les clients demandent surtout plus de stabilité. Nous préférons parfois des dashboards avec 5 minutes de retard plutôt que des incidents quotidiens.”
💬 Équipe Data Engineering
“Le streaming complet paraît séduisant, mais plusieurs pipelines Spark batch restent beaucoup plus simples à maintenir aujourd’hui.”
⚠️
Incidents
⚠️
Incident #245 – Rapport simplifié – Latence API
● Symptôme : temps de réponse > 5s
● Impact : clients bloqués
● Cause suspectée : surcharge base PostgreSQL
● Correctif temporaire : scaling manuel
⚠️
Incident #312 – Rapport simplifié – Perte de données batch
● Symptôme : données manquantes sur dashboard
● Cause inconnue
● Hypothèse : erreur dans pipeline ETL
⚠️
Extrait réunion incident
“L’origine exacte des incohérences entre dashboard et API n’est toujours pas confirmée.
Hypothèses évoquées :
● désynchronisation Kafka ;
● problème ETL ;
● cache backend ;
● duplication de traitements.”
📝 Notes internes diverses
📝 Équipe Engineering
● Kafka est utilisé uniquement par certains services
● Plusieurs flux passent encore directement par API
● Le Data Lake contient des données non nettoyées
📝 SRE
“Plusieurs équipes déploient encore leurs services avec des scripts Docker personnalisés. Les pratiques de déploiement restent hétérogènes.”
📊 Feedback interne – Data Scientist
“Le service de scoring est difficile à faire évoluer. On dépend trop des formats du Data Lake.”
📊 Note produit – Objectifs 2026
Objectifs stratégiques :
● doubler le nombre de clients enterprise ;
● réduire le délai de disponibilité des données analytiques ;
● permettre l’intégration de nouveaux modèles IA plus fréquemment.
📉 Note écologique (extrait)
● Coût de stockage en forte hausse
● Données rarement supprimées
● Redondance estimée à 30 %
🧩 Hypothèses évoquées (non validées)
● Migration vers architecture event-driven
● Remplacement du batch par streaming partiel
● Introduction d’un data warehouse