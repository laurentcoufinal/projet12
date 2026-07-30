# Cours message brokers + démos Angular

Cours et démos locales pour **Kafka** et **RabbitMQ** comme message brokers, avec intégration Angular.

## Contenu

| Fichier | Description |
|---------|-------------|
| [PROCEDURE_ARCHITECTURE_REPARTIE.md](PROCEDURE_ARCHITECTURE_REPARTIE.md) | Procédure conception d’archi répartie (outils, CI/CD, observabilité) |
| [COURS_KAFKA.md](COURS_KAFKA.md) | Cours Kafka (concepts, install, Angular via gateway) |
| [COURS_RABBITMQ.md](COURS_RABBITMQ.md) | Cours RabbitMQ (AMQP, STOMP, microservices, fédération) |
| [docker-compose.yml](docker-compose.yml) | Kafka + gateway FastAPI + RabbitMQ (management + STOMP) |
| [demo/](demo/) | Producteur / consommateur Kafka (Python CLI) |
| [gateway/](gateway/) | Bridge WebSocket → Kafka |
| [frontend/](frontend/) | Angular : onglets Kafka (gateway) et RabbitMQ (STOMP) |
| [rabbitmq/enabled_plugins](rabbitmq/enabled_plugins) | Plugins management + web-stomp |

## Prérequis

- Docker et Docker Compose
- Python 3.10+ (`python3-pip`, `python3-venv` sur Ubuntu/WSL)
- Node.js 18+ et npm

## Démarrage rapide — Angular + brokers

### 1. Lancer l’infra

```bash
docker compose up -d --build
```

| Service | URL / port |
|---------|------------|
| Kafka | `localhost:9092` |
| Gateway Kafka | `http://localhost:8000` — WS `ws://localhost:8000/ws` |
| RabbitMQ AMQP | `localhost:5672` |
| RabbitMQ Management | [http://localhost:15672](http://localhost:15672) (`guest`/`guest`) |
| RabbitMQ Web-STOMP | `ws://localhost:15674/ws` |

### 2. Lancer Angular

```bash
cd frontend
npm install
npm start
```

Ouvrir [http://localhost:4200](http://localhost:4200) :

- onglet **RabbitMQ (STOMP)** — connexion directe, file `/queue/demo-angular`
- onglet **Kafka (gateway)** — via FastAPI, topic `demo-events`

### 3. Arrêter

```bash
# Ctrl+C sur ng serve
docker compose down
```

## Démo Kafka Python (CLI)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r demo/requirements.txt
docker compose up -d kafka
python demo/consumer.py   # terminal A
python demo/producer.py   # terminal B
```

## Kafka vs RabbitMQ côté Angular

| | RabbitMQ | Kafka |
|--|----------|-------|
| Connexion UI | STOMP/WebSocket natif | Bridge WebSocket obligatoire |
| Composant | plugin `rabbitmq_web_stomp` | [`gateway/`](gateway/) FastAPI |

Détails : [COURS_RABBITMQ.md](COURS_RABBITMQ.md) et [COURS_KAFKA.md](COURS_KAFKA.md) §5.3.

## Gateway Kafka en local (sans image Docker)

```bash
docker compose up -d kafka
cd gateway
pip install -r requirements.txt
KAFKA_BOOTSTRAP=localhost:9092 uvicorn main:app --reload --port 8000
```
# projet12
