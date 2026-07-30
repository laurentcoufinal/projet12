# Frontend Angular — Kafka & RabbitMQ

Deux connexions persistantes pédagogiques :

| Onglet | Protocole | Cible |
|--------|-----------|--------|
| RabbitMQ (STOMP) | STOMP over WebSocket | `ws://localhost:15674/ws` → `/queue/demo-angular` |
| Kafka (gateway) | WebSocket JSON | `ws://localhost:8000/ws` → topic `demo-events` |

## Prérequis

```bash
# à la racine du dépôt
docker compose up -d --build
```

## Démarrage

```bash
npm install
npm start
```

Ouvrir http://localhost:4200

## Fichiers clés

- `src/app/rabbit-stomp.service.ts` — RxStomp vers RabbitMQ
- `src/app/kafka-bridge.service.ts` — WebSocket vers le gateway Kafka
- `src/app/app.component.*` — UI à onglets
