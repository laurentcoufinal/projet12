# Définition d’architecture du système cible — DonnÉlite

**Version :** 1.0 (finale)  
**Auteur :** Architecte logiciel — DonnÉlite  
**Destinataire :** Karyma, Head of Engineering  
**Date :** août 2026  
**Prérequis :** [01-rapport-audit.md](01-rapport-audit.md)  
**Suite :** [03-dossier-integration.md](03-dossier-integration.md)  
**Index / glossaire :** [README.md](README.md)

---

## 1. Objet du document

Ce document formalise l’architecture cible du SI DonnÉlite en réponse aux besoins d’évolution identifiés par l’audit. Il inclut :

- la modélisation du domaine métier ;
- la description de l’architecture cible ;
- un benchmark de solutions techniques ;
- la justification des choix retenus ;
- les interactions entre composants (existants et nouveaux) ;
- les critères d’acceptation.

**Principes directeurs :** migration progressive (pas de big-bang), réutilisation de Kafka / Spark / S3 / Gateway, maîtrise des coûts cloud, sobriété numérique (réduction duplication / rétention).

---

## 2. Modélisation du domaine métier

### 2.1 Bounded contexts

```mermaid
flowchart TB
  subgraph acquisition [Acquisition]
    sources[Sources_externes]
    contrats[Contrats_de_donnees]
  end
  subgraph plateforme [Plateforme_donnees]
    ingestion[Ingestion]
    traitement[Traitement_stream_et_batch]
    qualite[Qualite_et_zones]
  end
  subgraph produits [Produits_analytiques]
    indicateurs[Indicateurs_et_rapports]
    api[APIs_analytiques]
    selfserve[Self_service]
  end
  subgraph ia [Intelligence_artificielle]
    features[Features]
    modeles[Modeles_et_versions]
    scoring[Scoring_online_offline]
  end
  subgraph plateforme_tech [Plateforme_technique]
    identite[Identite_et_acces]
    obs[Observabilite]
    ops[Deploiement_et_run]
  end

  acquisition --> plateforme
  plateforme --> produits
  plateforme --> ia
  ia --> produits
  plateforme_tech --> plateforme
  plateforme_tech --> produits
  plateforme_tech --> ia
```

Version Draw.io : [diagrams/04-domaine-bounded-contexts.drawio](diagrams/04-domaine-bounded-contexts.drawio)

| Context | Responsabilité | Acteurs principaux |
|---------|----------------|-------------------|
| Acquisition | Connecter sources, valider schémas, garantir provenance | Data Engineering, partenaires |
| Plateforme données | Ingestion, transformation stream/batch, zones de qualité | Data Engineering |
| Produits analytiques | Indicateurs, API, dashboards, self-service | Produit, analystes, clients |
| IA | Features, entraînement, déploiement, scoring | Data Scientists, ML Eng. |
| Plateforme technique | Auth, monitoring, CI/CD, isolation multi-tenant légère | Engineering, SRE |

### 2.2 Concepts métier clés

| Concept | Définition |
|---------|------------|
| Source | Origine de données (API partenaire, CSV, app métier, événement) |
| Événement | Fait métier horodaté transitant sur le bus |
| Dataset de zone | Bronze (brut), Silver (nettoyé), Gold (servable) |
| Indicateur | Agrégat métier exposé dashboard / API |
| Modèle | Artefact versionné produisant un score |
| Contrat de données | Schéma + compatibilité + propriétaire |

### 2.3 Cas d’usage cibles couverts

| CU | Besoin audit / métier | Réponse architecturale |
|----|----------------------|------------------------|
| CU1 | Analyse quasi temps réel | Flink sur Kafka → couches Silver/Gold + alertes |
| CU2 | Intégration sources simplifiée | Kafka Connect + Schema Registry |
| CU3 | Déploiement fréquent des modèles | MLflow + serving découplé du brut |
| CU4 | Clients Enterprise | Serving ClickHouse, isolation, SLA, traçabilité |
| CU5 | Self-service analytics | Couche Gold gouvernée + outils BI sur ClickHouse |

---

## 3. Architecture du système cible

### 3.1 Vue d’ensemble

```mermaid
flowchart LR
  sources[Sources_clients] --> connectors[Kafka_Connect]
  connectors --> kafka[Kafka_plus_Schema_Registry]
  kafka --> stream[Apache_Flink]
  kafka --> batch[Spark_batch]
  stream --> lake[Data_Lake_S3_zones]
  batch --> lake
  lake --> warehouse[ClickHouse]
  kafka --> ml[MLflow_Serving]
  ml --> warehouse
  stream --> warehouse
  warehouse --> api[Backend_API_metier]
  api --> gateway[API_Gateway]
  gateway --> front[Frontend_dashboard]
  pg[(PostgreSQL_transactionnel)] --> api
  gateway -.-> otel[OTel_Collector]
  api -.-> otel
  kafka -.-> otel
  stream -.-> otel
  batch -.-> otel
  ml -.-> otel
  otel --> obs[Prometheus_Tempo_Loki_Grafana]
```

Version Draw.io : [diagrams/05-architecture-cible.drawio](diagrams/05-architecture-cible.drawio)

### 3.2 Composants : conservés, évolués, nouveaux

| Composant | Statut | Rôle cible |
|-----------|--------|------------|
| Frontend dashboard | Conservé | UI ; consomme API stables |
| API Gateway | Conservé | Entrée, auth, rate limiting Enterprise ; injection `traceparent` |
| Backend API métier | Évolué | Logique métier + lecture **ClickHouse** (analytique) ; PostgreSQL limité au transactionnel / métadonnées ; instrumentation OTel |
| Service ingestion RT | Évolué | Producteur Kafka standardisé (schémas) ; propagation trace dans headers |
| Kafka | Évolué / généralisé | Bus unique pour nouveaux flux ; fin progressive des contournements REST |
| **Schema Registry** | **Nouveau** | Contrats de schémas Avro/JSON Schema |
| **Kafka Connect** | **Nouveau** | Connecteurs sources (CDC, fichiers, HTTP) |
| ETL Spark batch | Conservé (périmètre réduit) | Jobs lourds historiques, repros, backfills ; métriques job + logs corrélés |
| **Apache Flink** | **Nouveau** | Traitements quasi temps réel, alertes |
| Data Lake S3 | Évolué | Zones bronze / silver / gold + TTL |
| PostgreSQL | Évolué (réduit) | Plus de serving analytique massif |
| **ClickHouse** | **Nouveau** | Serving analytique basse latence |
| Service scoring | Évolué | Consomme features / topics ; modèles via MLflow ; spans OTel |
| **MLflow** | **Nouveau** | Registry + déploiement versionné des modèles |
| Auth interne | Évolué | Renforcer isolation tenants / scopes API |
| **OpenTelemetry Collector** | **Nouveau** | Collecte unifiée traces / métriques / logs (OTLP) |
| **Prometheus** | **Nouveau** | Métriques et règles d’alerting SLO |
| **Grafana Tempo** | **Nouveau** | Stockage et requête des traces distribuées |
| **Grafana Loki** | **Nouveau** | Agrégation des logs corrélés aux `trace_id` |
| **Grafana** | **Nouveau** | Dashboards ops, exploration traces/logs, alertes |

### 3.3 Patterns architecturaux retenus

1. **Event-driven progressif (strangler fig)** — nouveaux flux et migrations via Kafka ; REST historiques encapsulés puis retirés.  
2. **Lambda pragmatique** — stream (Flink) pour fraîcheur ; batch (Spark) pour corrections et gros volumes.  
3. **Medallion (bronze / silver / gold)** — qualité progressive, moins de duplication anarchique.  
4. **CQRS léger** — écritures / événements séparés de la lecture analytique (ClickHouse).  
5. **Contrat-first data** — Schema Registry obligatoire pour les topics de production.  
6. **Observabilité by design** — trois piliers (métriques, traces, logs) corrélés via OpenTelemetry ; tout nouveau service expose OTLP avant mise en prod.

### 3.4 Flux cibles

#### Quasi temps réel (CU1)

Sources → Connect / ingestion → Kafka → Flink → Silver/Gold (S3) et/ou ClickHouse → API / alertes / dashboard.

#### Batch maîtrisé

Kafka ou lake bronze → Spark → silver/gold → ClickHouse (chargement périodique). Objectif : batch **&lt; 4 h**, plus de fenêtre saturée à 9h15.

#### Scoring IA (CU3)

Features (topics ou tables gold) → MLflow Serving / scoring → résultats vers Kafka et/ou ClickHouse. Le scoring **ne lit plus** le brut non contractuel comme dépendance principale.

#### Lecture Enterprise (CU4)

Gateway → Backend → ClickHouse (analytique) + PostgreSQL (état applicatif). Isolation par projet / tenant au niveau requêtes et quotas Gateway.

#### Observabilité répartie (réponse audit R7 / besoin P1)

```mermaid
flowchart TB
  services[Gateway_Backend_Ingestion_Flink_Spark_Scoring]
  services -->|OTLP| collector[OTel_Collector]
  collector --> prom[Prometheus]
  collector --> tempo[Grafana_Tempo]
  collector --> loki[Grafana_Loki]
  prom --> grafana[Grafana]
  tempo --> grafana
  loki --> grafana
  grafana --> alertes[Alertes_SLO_API_lag_Kafka_batch]
```

Version Draw.io : [diagrams/08-observabilite-repartie.drawio](diagrams/08-observabilite-repartie.drawio)

**Propagation de contexte :**

- HTTP : W3C Trace Context (`traceparent` / `tracestate`) dès l’API Gateway.  
- Kafka : report du `traceparent` dans les **headers de message** (producteurs Node/Python ; consommateurs scoring / jobs).  
- Logs structurés JSON avec champ `trace_id` pour corrélation Loki ↔ Tempo.  

**Alertes minimales cibles :** latence API p95, taux d’erreur 5xx, lag consumer Kafka, durée / échec jobs Spark, DLQ non vide, saturation Collector.

---
## 4. Benchmark de solutions techniques

### 4.1 Serving analytique (réponse à la saturation PostgreSQL)

| Critère | PostgreSQL seul (statut quo) | Snowflake | **ClickHouse** |
|---------|------------------------------|-----------|----------------|
| Latence agrégats massifs | Faible à moyenne sous charge | Bonne | Excellente |
| Coût à volume DonnÉlite | CPU/IO élevés, scaling vertical limité | Élevé (SaaS) | Maîtrisé (self-hosted / cloud) |
| Intégration Kafka / S3 | Indirecte | Native cloud | Connecteurs / engins matures |
| Compétences équipe | Fortes | À acquérir | À monter (SQL proche) |
| Migration progressive | N/A | Remplacement net | Couche serving ajoutée |
| Green IT | Mauvaise si surcharge + retries | Dépend usage | Moins de compute pour mêmes agrégats |

**Retenu : ClickHouse** — meilleur compromis perf / coût / migration progressive. Snowflake écarté (coût et dépendance). Statu quo PostgreSQL écarté (R1/R2 audit).

### 4.2 Traitement quasi temps réel

| Critère | Spark Structured Streaming | Kafka Streams | **Apache Flink** |
|---------|----------------------------|---------------|------------------|
| Latence | Moyenne | Bonne | Excellente |
| Complexité ops | Équipe déjà Spark | Légère | Moyenne |
| Fenêtres / état | Bon | Bon (périmètre JVM) | Excellent |
| Alignement batch existant | Fort | Faible | Complémentaire |
| Cas alertes critiques | Possible | Possible | Adapté |

**Retenu : Apache Flink** pour le chemin « minutes ». Spark batch **conservé** pour les pipelines stables (souhait Data Engineering). Kafka Streams écarté comme moteur principal (moins adapté multi-langage / jobs data complexes DonnÉlite).

### 4.3 Intégration de sources

| Critère | Développements REST ad hoc | **Kafka Connect + Schema Registry** | iPaaS propriétaire |
|---------|----------------------------|--------------------------------------|--------------------|
| Time-to-source | Lent | Rapide si connecteur | Rapide |
| Gouvernance schémas | Faible | Forte | Variable |
| Coût licence | Faible | Faible (OSS) | Élevé |
| Alignement Kafka existant | Faible | Fort | Moyen |

**Retenu : Kafka Connect + Schema Registry.**

### 4.4 Cycle de vie ML

| Critère | Scoring actuel (collé au lake) | SageMaker / Vertex | **MLflow** |
|---------|--------------------------------|--------------------|------------|
| Déploiement versionné | Difficile | Oui | Oui |
| Lock-in cloud | Non | Fort | Faible |
| Coût | Caché (ops) | Élevé | Maîtrisé |
| Intégration custom | Limitée | Bonne | Bonne |

**Retenu : MLflow** (+ service de scoring évolué). Cloud ML managé écarté pour limiter coûts et lock-in à ce stade.

### 4.5 Observabilité répartie (réponse R7 / P1 audit)

| Critère | Datadog (APM SaaS) | ELK complet | **OTel + Prometheus + Tempo + Loki + Grafana** |
|---------|--------------------|-------------|--------------------------------------------------|
| Traces distribuées | Oui | Partiel / complexe | Oui (Tempo) |
| Corrélation logs ↔ traces | Oui | Possible | Native via `trace_id` |
| Coût à l’échelle DonnÉlite | Élevé (ingestion) | Infra lourde | Maîtrisé (OSS) |
| Lock-in | Fort | Moyen | Faible (OTLP standard) |
| Fit Node.js / Python | Excellent | Bon | Excellent (SDK OTel) |
| Compétences SRE | Faibles à monter | Fortes ELK | Montée Grafana déjà courante |

**Retenu : OpenTelemetry + Prometheus + Grafana Tempo + Loki + Grafana.** Datadog écarté (coût / dépendance). ELK complet écarté comme socle unique (complexité ops et moins adapté au tracing distribué moderne).

### 4.6 Synthèse du benchmark

| Besoin audit | Solution retenue | Alternative écartée |
|--------------|------------------|---------------------|
| Décharge PostgreSQL / SLA API | ClickHouse | Snowflake ; PG seul |
| Quasi temps réel | Apache Flink | Streaming Spark seul |
| Intégration sources | Kafka Connect + Schema Registry | REST ad hoc |
| Modèles IA | MLflow + serving découplé | Statu quo lake brut |
| Qualité / traçabilité | Contrats de schéma + zones | Status quo |
| Observabilité / MTTR (R7) | OTel + Prometheus + Tempo + Loki + Grafana | Datadog ; ELK seul |
| Green IT | Zones + TTL + dédup | Stockage sans rétention |

---
## 5. Justification architecturale globale

### 5.1 Réponse aux risques P0 de l’audit

| Risque audit | Mesure cible |
|--------------|--------------|
| R1 SLA API | ClickHouse + quotas Gateway ; PG hors chemin analytique critique |
| R2 Batch saturé | Flink pour l’essentiel « frais » ; Spark recentré ; SLO batch &lt; 4 h |
| R3 Incohérences | Bus Kafka généralisé + schémas ; une couche Gold pour dashboard et API |
| R7 Angle mort monitoring | Socle OTel + Grafana ; couverture ≥ 95 % ; traces bout-en-bout |

### 5.2 Respect des contraintes métier

- **Économique :** OSS self-hostable (ClickHouse, Flink, MLflow, Kafka, stack Grafana/OTel) ; réduction duplication → moins de stockage.  
- **Opérationnelle :** strangler fig ; coexistence REST temporaire.  
- **Organisationnelle :** contrats de données → autonomie Data / DS ; runbooks Grafana pour SRE.  
- **Durabilité :** medallion + TTL + moins de recomputes sur échecs batch ; rétention Tempo/Loki bornée.  
- **Produit :** fraîcheur en minutes **avec** stabilité (préférée à un streaming total fragile).

### 5.3 Hypothèses de conception

1. Les équipes peuvent monter en compétence Flink / ClickHouse / OTel avec accompagnement (spike + formation).  
2. Kafka cluster actuel (ou son successeur) peut être dimensionné pour devenir le bus canonique.  
3. Une partie des indicateurs Enterprise peut tolérer ~5 minutes de latence (retour Produit).  
4. PostgreSQL reste pertinent pour les données transactionnelles / métadonnées applicatives.  
5. Les services Node.js et Python peuvent être instrumentés via SDK OpenTelemetry sans réécriture majeure.

---

## 6. Interactions entre composants (existant + nouveau)

### 6.1 Matrice d’intégration

| Nouveau / évolué | Interagit avec | Mode d’interaction |
|------------------|----------------|--------------------|
| Schema Registry | Ingestion, Connect, Flink, Spark, Scoring | API registre ; sérialisation Avro/JSON Schema |
| Kafka Connect | Sources externes, Kafka | Connecteurs source → topics |
| Flink | Kafka, S3, ClickHouse | Consommation topics ; écriture lake / CH |
| ClickHouse | Flink, Spark, Backend | Inserts batch/stream ; SQL via backend |
| MLflow | Scoring, Data Scientists, CI | Registry modèles ; endpoints serving |
| Kafka généralisé | Backend, ETL, Scoring | Remplace progressivement REST internes data |
| OTel Collector | Gateway, Backend, Ingestion, Flink, Spark, Scoring, MLflow | OTLP (traces, metrics, logs) |
| Prometheus / Tempo / Loki | OTel Collector, Grafana | Stockage et requête des trois piliers |
| Grafana | Prometheus, Tempo, Loki, SRE | Dashboards, alertes, corrélation |
### 6.2 Séquence — nouvel indicateur quasi temps réel

```mermaid
sequenceDiagram
  participant Src as Source_client
  participant KC as Kafka_Connect
  participant K as Kafka
  participant SR as Schema_Registry
  participant F as Flink
  participant CH as ClickHouse
  participant API as Backend
  participant UI as Dashboard

  Src->>KC: Evenements
  KC->>SR: Valide_schema
  KC->>K: Produce_topic
  F->>K: Consume
  F->>CH: Upsert_agregats
  UI->>API: GET_indicateur
  API->>CH: Requete_analytique
  CH-->>API: Resultat
  API-->>UI: JSON
```

Version Draw.io : [diagrams/06-sequence-indicateur-temps-reel.drawio](diagrams/06-sequence-indicateur-temps-reel.drawio)

### 6.3 Coexistence pendant la migration

| Phase | État |
|-------|------|
| 0 | Audit / socle Kafka + Schema Registry + **Collector OTel + Grafana** (métriques/lag minimaux) |
| 1 | Nouveaux flux via Kafka + schémas ; ClickHouse en lecture parallèle ; traces Gateway→Backend |
| 2 | Basculer API analytiques critiques vers ClickHouse ; Flink ; propagation `traceparent` Kafka |
| 3 | Réduire REST data path ; Spark recentré ; MLflow ; couverture OTel ≥ 95 % services |
| 4 | TTL lake, dédup, self-service sur Gold ; rétention Tempo/Loki optimisée (Green IT) |

Le détail opérationnel de la phase 0–1 pour Kafka / Schema Registry est dans [03-dossier-integration.md](03-dossier-integration.md). L’observabilité Kafka (lag, DLQ) s’adosse au socle OTel décrit en §3.4.

---

## 7. Critères d’acceptation

La solution cible est considérée **validée** lorsque les critères suivants sont mesurés et atteints (environnement de staging puis production par vagues).

### 7.1 Critères fonctionnels

| ID | Critère | Mesure | Seuil |
|----|---------|--------|-------|
| AF1 | Indicateurs prioritaires quasi temps réel | Délai événement → disponibilité dashboard/API | ≤ 5 minutes (p95) |
| AF2 | Intégration d’une nouvelle source « standard » | Délai calendaire sans refonte d’architecture | ≤ 5 jours ouvrés (connecteur existant) |
| AF3 | Déploiement d’une nouvelle version de modèle | Via MLflow jusqu’à scoring en staging | ≤ 1 jour ouvré |
| AF4 | Dashboard et API sur même indicateur Gold | Écart relatif | &lt; 0,1 % ou égalité bit-à-bit sur jeu de test |
| AF5 | Traçabilité | Présence schéma + producteur + version modèle sur flux critiques | 100 % des topics prod critiques |

### 7.2 Critères non fonctionnels

| ID | Critère | Seuil |
|----|---------|-------|
| AN1 | Latence API analytique (p95) hors incident | ≤ 500 ms |
| AN2 | Disponibilité API analytiques (mensuelle) | ≥ 99,9 % |
| AN3 | Durée pipeline batch résiduel | ≤ 4 h |
| AN4 | Taux de services instrumentés OTel (métriques + logs structurés + alerting Grafana) | ≥ 95 % |
| AN5 | Volume de données dupliquées inutiles | ≤ 10 % (vs ~30 % aujourd’hui) |
| AN6 | Continuité de service pendant bascules | Aucune interruption non planifiée &gt; RTO convenu (cible 15 min) |
| AN7 | Trace distribuée bout-en-bout sur flux pilote (Gateway → Backend → Kafka → consommateur) | Visible dans Tempo pour ≥ 99 % des requêtes test |
| AN8 | MTTD sur dépassement latence API (alerte Grafana) | ≤ 5 minutes |
| AN9 | Corrélation log ↔ trace (`trace_id`) sur services critiques | 100 % des services P0 instrumentés |

### 7.3 Critères Green IT

| ID | Critère | Seuil |
|----|---------|-------|
| AG1 | Politique de rétention documentée et appliquée (lake + topics + Tempo/Loki) | Oui |
| AG2 | Réduction stockage redondant | −50 % du volume dupliqué identifié sous 12 mois |
| AG3 | Jobs batch en échec relancés automatiquement (idempotents) | ≥ 90 % sans intervention humaine |

### 7.4 Critères d’acceptation du socle d’intégration (composant Kafka / Schema Registry)

Détaillés et testables dans le dossier d’intégration ; résumé :

- Un producteur et un consommateur de référence valident un contrat de schéma en environnement de dev.  
- Un flux pilote n’utilise plus le contournement REST.  
- Documentation d’onboarding exécutable sans l’architecte.

---

## 8. Roadmap indicative

| Vague | Objectif | Composants |
|-------|----------|------------|
| V0 | Socle événements + observabilité | Kafka généralisé, Schema Registry, OTel Collector, Prometheus, Grafana (métriques/lag) |
| V1 | Serving, SLA et traces HTTP | ClickHouse, bascule lectures API, Tempo + instrumentation Gateway/Backend |
| V2 | Fraîcheur + propagation Kafka | Flink sur top N indicateurs, `traceparent` sur messages, Loki |
| V3 | Sources & IA | Kafka Connect, MLflow, scoring découplé, couverture OTel élargie |
| V4 | Sobriété & self-service | TTL, dédup, BI sur Gold, rétention Tempo/Loki bornée |

---

## 9. Conclusion

L’architecture cible conserve le cœur utile de DonnÉlite (Kafka, Spark, S3, Gateway, backend, frontend) et y ajoute une **colonne vertébrale événementielle gouvernée** (Schema Registry, Connect), un **moteur stream** (Flink), un **serving analytique** (ClickHouse), une **usine ML** (MLflow) et un **socle d’observabilité répartie** (OpenTelemetry, Prometheus, Tempo, Loki, Grafana). Ce design répond aux risques critiques de l’audit — y compris le monitoring partiel (R7) — tout en respectant migration progressive, coûts et Green IT.

Prochaine étape opérationnelle : intégrer en premier le socle **Kafka + Schema Registry** (avec métriques de lag branchées sur Grafana) — [03-dossier-integration.md](03-dossier-integration.md).
