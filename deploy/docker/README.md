# Running permafrost in Docker

## Build

From the **repository root** (the Dockerfile expects the workspace as its build
context):

```bash
docker build -f deploy/docker/Dockerfile -t permafrost:latest .
```

> **Add a `.dockerignore` first.** Without one, the build context is the whole
> working tree — including `target/`, which is several GB after a local
> `cargo build`, and `web/vendor/`, which is ~74 MB of DuckDB wasm. None of it
> is used by the image. Create `.dockerignore` at the **repository root**:
>
> ```gitignore
> target/
> web/vendor/
> .git/
> .omc/
> data/
> ```

## Run

```bash
mkdir -p ./data ./rules

docker run -d --name permafrost \
  -p 3100:3100 \
  -v "$PWD/data:/data" \
  -v "$PWD/rules:/rules:ro" \
  -e PERMAFROST_SLACK_WEBHOOK="https://hooks.slack.com/services/T000/B000/XXXX" \
  -e PERMAFROST_UI_BASE_URL="https://permafrost.example.com/" \
  permafrost:latest
```

Then point promtail at it — one line, see
[`../promtail-example.yaml`](../promtail-example.yaml):

```yaml
clients:
  - url: http://collector-host:3100/loki/api/v1/push
```

Check it is up:

```bash
curl -fsS http://localhost:3100/ready
curl -fsS http://localhost:3100/metrics | head
```

## The two env vars that matter

| Variable | Why it is required |
|---|---|
| `PERMAFROST_SLACK_WEBHOOK` | Incoming-webhook URL alerts are posted to. Without it, rules still run but nothing is delivered. |
| `PERMAFROST_UI_BASE_URL` | Public base URL of the static page in `web/`. The deep link is built from it; the ticket goes in the **fragment**, so it is never sent to that page's HTTP server. |

Never bake either into the image — pass them at `docker run` time (`-e`, or
`--env-file .env`), and note that permafrost never logs webhook URLs or ticket
contents at info level.

Everything else has a working default; see README §Configuration Reference for
the full list (`PERMAFROST_SEGMENT_MAX_AGE`, `PERMAFROST_RETENTION`,
`PERMAFROST_RULE_FUEL`, …).

## Volumes

- **`/data`** (declared `VOLUME`) — Parquet segments, the iroh blob store, and
  `iroh.key`. **Persist this.** `iroh.key` is the node identity; losing it
  invalidates every deep link already sitting in Slack history.
- **`/rules`** — `.wasm` rule modules, hot-reloaded on change. Mounting
  read-only (`:ro`) is fine and recommended.

The container runs as non-root uid/gid `10001`. If your bind-mounted `./data`
is owned by someone else, fix it once on the host:

```bash
sudo chown -R 10001:10001 ./data
```

## Networking

Publishing `3100` is for **ingest only** — that is promtail talking to the
collector.

The browser query path needs **no inbound port**. iroh dials out: outbound UDP
for QUIC and hole-punching, plus HTTPS egress to the n0 relays, falling back to
relay-only (still end-to-end encrypted) when UDP is blocked. Default bridge
networking already allows this.

So you can firewall off everything inbound except 3100 and the Slack deep link
still resolves in a browser — that is the acceptance criterion proving there is
no HTTP query path.

## Logs and lifecycle

```bash
docker logs -f permafrost
docker stop permafrost && docker rm permafrost
```

## docker compose

```yaml
services:
  permafrost:
    build:
      context: ../..
      dockerfile: deploy/docker/Dockerfile
    ports:
      - "3100:3100"
    volumes:
      - ./data:/data
      - ./rules:/rules:ro
    environment:
      PERMAFROST_SLACK_WEBHOOK: ${PERMAFROST_SLACK_WEBHOOK:?set me}
      PERMAFROST_UI_BASE_URL: ${PERMAFROST_UI_BASE_URL:?set me}
    restart: unless-stopped
```
