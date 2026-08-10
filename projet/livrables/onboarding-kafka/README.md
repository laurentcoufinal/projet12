# Onboarding local — Kafka + Schema Registry (DonnÉlite)

Environnement de développement reproductible pour le socle événementiel décrit dans le [dossier d’intégration](../03-dossier-integration.md).

Objectif : démarrer, publier et consommer un événement **avec contrat de schéma**, sans aide externe.

---

## Prérequis

- Docker Engine + Docker Compose v2
- Python 3.10+
- Ports libres : **9092** (Kafka), **8081** (Schema Registry)

Vérification rapide :

```bash
docker --version
docker compose version
python3 --version
```

---

## 1. Démarrer l’infrastructure

Depuis ce répertoire (`projet/livrables/onboarding-kafka`) :

```bash
cp .env.example .env
docker compose up -d
docker compose ps
```

Services attendus :

| Service | URL |
|---------|-----|
| Kafka | `localhost:9092` |
| Schema Registry | http://localhost:8081 |

Smoke test Registry :

```bash
curl -s http://localhost:8081/subjects
# attendu : []  au premier démarrage
```

Attendre ~30 s si Kafka n’est pas encore prêt.

---

## 2. Préparer l’environnement Python

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r examples/requirements.txt
```

---

## 3. Publier un événement (producteur)

```bash
python examples/producer.py
```

Comportement :

1. Enregistre le schéma Avro `logistique.shipment_event.v1-value` auprès du Registry  
2. Publie un message sur le topic `logistique.shipment_event.v1`  
3. Affiche l’`event_id` publié  

Vérifier le subject :

```bash
curl -s http://localhost:8081/subjects | python3 -m json.tool
```

---

## 4. Consommer l’événement

Dans un second terminal (même venv activé) :

```bash
python examples/consumer.py
```

Attendu : affichage JSON de l’événement (champs `event_id`, `event_time`, `tenant_id`, `shipment_id`, etc.).

Arrêt : `Ctrl+C`.

---

## 5. Démontrer la compatibilité BACKWARD

Le Registry est configuré en `BACKWARD` (voir `config/schema-registry.properties`).

1. Ouvrir `examples/schemas/shipment_event.avsc`  
2. **Supprimer** un champ existant (évolution incompatible en BACKWARD)  
3. Relancer `python examples/producer.py`  

Attendu : **erreur d’enregistrement de schéma** (incompatibilité).  

Remettre ensuite le fichier schéma d’origine (ou `git checkout -- examples/schemas/shipment_event.avsc`).

Évolution **autorisée** : ajouter un champ avec `"default"`.

---

## 6. Arrêter et nettoyer

```bash
docker compose down
# suppression des volumes (données locales) :
docker compose down -v
```

---

## Variables d’environnement

Voir [`.env.example`](.env.example). Principales :

| Variable | Défaut | Rôle |
|----------|--------|------|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Brokers |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Registry |
| `TOPIC_NAME` | `logistique.shipment_event.v1` | Topic pilote |

---

## Dépannage

| Symptôme | Piste |
|----------|--------|
| `Connection refused` :9092 | Attendre le healthcheck ; `docker compose logs kafka` |
| `NoBrokersAvailable` | Vérifier `KAFKA_BOOTSTRAP` et `docker compose ps` |
| Erreur sérialisation Avro | Schema Registry down ; `curl localhost:8081` |
| Port déjà utilisé | Changer les ports dans `docker-compose.yml` / `.env` |

---

## Critères « onboarding OK »

- [ ] `curl` Registry répond  
- [ ] Producteur publie sans erreur  
- [ ] Consommateur affiche l’événement  
- [ ] Schéma incompatible rejeté  

Temps cible pour un nouvel arrivant : **moins d’une heure**.
