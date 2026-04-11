# GitLab Issue Ingestion Layer

> **Status:** Ingestion layer verified awaiting plugging the orchestrator service.



## Architecture Overview

7ta n9ad schema :D

All services communicate over a private Docker bridge network (`agent-net`). Nothing is exposed to the internet except the webhook listener, which sits behind your reverse proxy.

## Services

### `webhook_listener`

The entry point. A FastAPI application that:

1. **Validates** the `X-Gitlab-Token` header against `GITLAB_SECRET_TOKEN`.
2. **Filters** events to `issue` object kinds only, and only for actions `open`, `update`, `close`, `reopen`. (delete to be considered later)
3. **Publishes raw** payload to `events.raw` immediately before normalisation. This means you always have a pristine copy even if parsing fails.
4. **Normalises** the payload:

>  This preliminary i'll change it when we agree on an issue format

   - Parses a structured title convention: `[AGENT:intent] scope :: summary`
   - Extracts labelled sections from the description body: `---context---`, `---acceptance_criteria---`, `---constraints---`, `---non_goals---`, `---agent_hints---`
   - Parses agent hints for `priority:`, `scope:`, `depends_on:`, `branch:`
   - Derives a `routing_key` in the form `intent.priority.action` (e.g. `fix.high.open`)
5. **Publishes normalised** event to `events.normalized`.
6. On normalisation failure, publishes to `events.failed` and continues — the raw event is always safe.

The webhook endpoint returns `202 Accepted` immediately; publishing happens in a background task so GitLab never times out.

**Health check:** `GET /health` → `{"status": "ok"}`

### `consumer`

Reads from `events.normalized` and dispatches to the orchestrator. It:

- Filters to `open` and `update` actions only.
- POSTs a structured `ticket` object to the orchestrator's `/orchestrate` endpoint.
- Uses Kafka consumer group `orchestrator-consumer`, so offset is tracked and messages are not reprocessed on restart.

The ticket payload it sends:
> Again preliminary to be changed
```json
{
  "ticket": {
    "intent": "fix",
    "title": "...",
    "description": "...",
    "summary": "...",
    "scope": "...",
    "acceptance_criteria": ["..."],
    "constraints": "...",
    "repo_url": "https://gitlab.com/org/repo.git",
    "issue_id": 123,
    "url": "https://gitlab.com/...",
    "author": "username",
    "branch": "main",
    "labels": ["bug", "priority::high"],
    "project": "my-project",
    "priority": "high"
  }
}
```

### `repo-syncer`

Reads from `events.normalized` in parallel with the consumer (separate consumer group: `repo-syncer`). For each `open` or `update` event, it:

- Clones the repo.
- Fetches and resets to `origin/<branch>` (or `origin/HEAD` if no branch is specified) if the repo exists.
- Authenticates using `oauth2:<GITLAB_TOKEN>` injected into the clone URL.

Cloned repos land in `.workdir/<repo-name>/` on the host, mounted into the container at `/repos`.

### Kafka

Single-broker Kafka (Confluent Platform 7.5) with Zookeeper. Topics are created by `kafka-init` at startup using a shell script (`infra/kafka/topics.sh`). Auto-creation is disabled — topics must be defined explicitly.

Expected topics:

| Topic | Purpose |
|---|---|
| `events.raw` | Raw GitLab webhook payloads, always written |
| `events.normalized` | Parsed, structured events |
| `events.failed` | Events that failed normalisation |

**Kafka UI** is available at `http://localhost:8080` for inspecting messages, consumer group lag, and topic contents during development.

## Exposing the Webhook

The webhook listener runs on port `8000` inside Docker. GitLab needs to reach it over HTTPS. Two recommended approaches:

## Configuration

All secrets and config go in a `.env` file at the project root. Never commit this file.

```env
# Required — pick any strong random string, then configure the same value in GitLab
GITLAB_SECRET_TOKEN=your-secret-token-here

# Required — your GitLab Personal Access Token with read_repository scope
# The one in the repo was used for testing; replace it with your own
GITLAB_TOKEN=glpat-xxxxxxxxxxxxxxxxxxxx

# Optional — defaults shown
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ORCHESTRATOR_URL=http://orchestrator:8000/orchestrate
```

### Setting up your GitLab webhook

1. Go to your GitLab project → **Settings → Webhooks**
2. Set **URL** to your public webhook URL (Tailscale or Nginx)
3. Set **Secret token** to the same value as `GITLAB_SECRET_TOKEN` in your `.env`
4. Enable the **Issues events** trigger
5. Click **Add webhook**
6. Use **Test → Issues events** to send a test payload

### GitLab PAT (`GITLAB_TOKEN`)

The token in the repository was used for initial testing. You should create your own:

1. GitLab → **User Settings → Access Tokens**
2. Name: `agent-repo-syncer` (or anything)
3. Scopes: `read_repository` is sufficient
4. Set it as `GITLAB_TOKEN` in your `.env`

The token is only used by `repo-syncer` to authenticate git clones over HTTPS. If your repos are public, you can leave `GITLAB_TOKEN` empty.

## Directory Structure

```
.
├── .env                        
├── .workdir/                   
├── docker-compose.yml
├── infra/
│   └── kafka/
│       └── topics.sh          
└── services/
    ├── webhook_listener/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── main.py
    ├── consumer/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── consumer.py
    └── repo_syncer/
        ├── Dockerfile
        ├── requirements.txt
        └── repo_syncer.py
```