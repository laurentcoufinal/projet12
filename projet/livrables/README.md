# Livrables CDC — DonnÉlite

Mission Architecture (évolution du SI) — réponses aux besoins du [CDC](../CDC.md).

| # | Livrable | Fichier | Statut |
|---|----------|---------|--------|
| 1 | Rapport d’audit du SI existant | [01-rapport-audit.md](01-rapport-audit.md) | Final |
| 2 | Définition d’architecture cible | [02-definition-architecture.md](02-definition-architecture.md) | Final |
| 3 | Dossier d’intégration (Kafka + Schema Registry) | [03-dossier-integration.md](03-dossier-integration.md) | Final |
| 3b | Onboarding environnement de développement | [onboarding-kafka/README.md](onboarding-kafka/README.md) | Final |

## Sources starter kit

- [Rapport technique architecture](../papport%20technique%20architecture.md)
- [Contexte métier](../contexte%20metier.md)
- [Incidents et retours](../incident.md)
- [Schéma UML actuel](../image.png)

## Glossaire commun

| Terme | Définition partagée dans les 3 livrables |
|-------|------------------------------------------|
| Bronze / Silver / Gold | Zones Data Lake (brut → nettoyé → servable) |
| Strangler fig | Migration progressive en encapsulant l’existant |
| Schema Registry | Service de contrats de schémas (Avro/JSON Schema) |
| Serving analytique | Couche de lecture optimisée (cible : ClickHouse) |
| Flux pilote | Premier flux migré REST → Kafka pour valider le socle |
| P0 / P1 / P2 | Priorités des besoins d’évolution (audit §9) |
| BACKWARD | Compatibilité de schéma : nouveaux schémas lisibles par consommateurs anciens |
| Observabilité répartie | Traces + métriques + logs corrélés via OpenTelemetry sur l’ensemble des services |
| OTLP | Protocole d’export OpenTelemetry vers le Collector |

## Hypothèses transverses

1. Documents internes partiels / parfois obsolètes — les métriques à ~ sont estimées.
2. Kafka partiel + contournements REST sont un constat central de l’audit.
3. Migration sans big-bang ; continuité de service obligatoire.
4. Premier composant intégré en détail : **Kafka + Schema Registry** (prérequis Flink / ClickHouse / MLflow).
5. Socle d’observabilité répartie : **OpenTelemetry + Prometheus + Tempo + Loki + Grafana** (dès V0/V1).

## Ordre de lecture recommandé

1. Audit → comprendre limites et besoins  
2. Architecture → choix et critères d’acceptation  
3. Intégration + onboarding → mise en œuvre concrète du socle

## Diagrammes Draw.io

Éditables avec [diagrams.net](https://app.diagrams.net/) ou l’extension VS Code / Cursor Draw.io. Les versions Mermaid restent dans les livrables Markdown.

| Fichier | Contenu | Livrable |
|---------|---------|----------|
| [diagrams/01-c4-contexte-actuel.drawio](diagrams/01-c4-contexte-actuel.drawio) | C4 contexte (acteurs → plateforme) | Audit §3.1 |
| [diagrams/02-c4-conteneurs-actuel.drawio](diagrams/02-c4-conteneurs-actuel.drawio) | C4 conteneurs (architecture actuelle) | Audit §3.2 |
| [diagrams/03-dependances-critiques.drawio](diagrams/03-dependances-critiques.drawio) | Couplages à risque | Audit §6.3 |
| [diagrams/04-domaine-bounded-contexts.drawio](diagrams/04-domaine-bounded-contexts.drawio) | Bounded contexts métier | Architecture §2.1 |
| [diagrams/05-architecture-cible.drawio](diagrams/05-architecture-cible.drawio) | Architecture cible (Flink, ClickHouse, MLflow, OTel…) | Architecture §3.1 |
| [diagrams/06-sequence-indicateur-temps-reel.drawio](diagrams/06-sequence-indicateur-temps-reel.drawio) | Séquence indicateur quasi temps réel | Architecture §6.2 |
| [diagrams/07-compatibilite-integration-kafka.drawio](diagrams/07-compatibilite-integration-kafka.drawio) | Compatibilité SI ↔ Kafka / Schema Registry | Intégration §3.4 |
| [diagrams/08-observabilite-repartie.drawio](diagrams/08-observabilite-repartie.drawio) | Observabilité répartie (OTel, Prometheus, Tempo, Loki, Grafana) | Architecture §3.4 |
