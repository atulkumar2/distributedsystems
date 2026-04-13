# Infra Stack Notes

This folder defines local shared infrastructure used across stages in this repo.

## Centralized settings

All tunable names and ports live in `.env` in this folder.

Bootstrap it from the committed template before first run:

```bash
cp .env.template .env
```

Use that file to change:

- container names
- published host ports
- shared network name
- Postgres and Portainer volume names
- Postgres credentials and database name

## Why split into two compose files

- `docker-compose.yml` is the core stack:
  - Postgres (`${POSTGRES_CONTAINER_NAME}`) on host port `${POSTGRES_HOST_PORT}`
  - Kafka (`${KAFKA_CONTAINER_NAME}`) on host port `${KAFKA_HOST_PORT}`
  - Kafka UI (`${KAFKA_UI_CONTAINER_NAME}`) on host port `${KAFKA_UI_HOST_PORT}`
- `docker-compose.tools.yml` is optional tooling:
  - Adminer (`${ADMINER_CONTAINER_NAME}`) on host port `${ADMINER_HOST_PORT}`
  - Portainer (`${PORTAINER_CONTAINER_NAME}`) on host ports `${PORTAINER_HTTP_PORT}` and `${PORTAINER_HTTPS_PORT}`

This split keeps day-to-day runtime lighter and avoids always running admin tools.

## Network choice

The tools compose file uses:

- `external: true`
- network name comes from `.env` (`SHARED_NETWORK_NAME`)

Reason:

- Core and tools are in separate compose files.
- Adminer needs to reach the Postgres container by name (`POSTGRES_CONTAINER_NAME`).
- A shared external network lets both stacks communicate cleanly.

Note:

- Start core stack first, then tools stack, so the shared network already exists.

## Volume naming choices

- Postgres data volume name comes from `POSTGRES_VOLUME_NAME` for clear ownership.
- Portainer data volume name comes from `PORTAINER_VOLUME_NAME` because it is intended to be reusable globally on this Docker host.

## Global tools vs per-project tools

- Portainer is host-global because it mounts Docker socket and can inspect all containers on this Docker engine.
- Adminer is a DB client and only connects where network/ports allow.

Current decision:

- Keep this shared tools file as-is.
- For multi-Postgres workflows, create one Adminer service per Postgres/project when needed.

## Commands

Run from this folder:

```bash
./run.sh --start
./run.sh --start-tools
./run.sh --stop-tools
./run.sh --stop
./run.sh --stop-blockers
```

## Connection hints

- Postgres URI:
  - `postgres://<POSTGRES_USER>:<POSTGRES_PASSWORD>@localhost:<POSTGRES_HOST_PORT>/<POSTGRES_DB>`
- Adminer default server in tools stack:
  - `ADMINER_DEFAULT_SERVER`
