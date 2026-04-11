import json
import uuid
import hmac
import logging
import re
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import Any
import asyncio

from fastapi import FastAPI, Request, Header, HTTPException, BackgroundTasks
from aiokafka import AIOKafkaProducer
from pydantic import BaseModel
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    gitlab_secret_token: str
    kafka_bootstrap_servers: str = "kafka:9092"

    class Config:
        env_file = ".env"


settings = Settings()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TRACKED_ACTIONS = {"open", "update", "close", "reopen"}


class RawEvent(BaseModel):
    event_id: str
    received_at: str
    action: str
    payload: dict[str, Any]


class Event(BaseModel):
    event_id: str
    issue_id: int
    project: str

    title: str
    intent: str | None
    scope: str | None
    summary: str | None

    description: str
    context: str | None
    acceptance_criteria: list[str]
    constraints: str | None
    non_goals: str | None

    priority: str
    hinted_scope: list[str]
    depends_on: list[int]
    branch: str | None

    routing_key: str
    action: str

    labels: list[str]
    author: str
    url: str
    created_at: str
    received_at: str
    updated_at: str
    path_with_namespace: str


def clean_description(text: str) -> str:
    text = text.replace("\\\\\n", "\n")
    text = text.replace("\\\\", "")
    text = text.replace("\\\n", "\n")
    text = re.sub(r"\\([#\[\]\-`*_>])", r"\1", text)
    text = re.sub(r"^\* ", "- ", text, flags=re.MULTILINE)

    text = re.sub(r"\s*---([\w]+)---\s*", r"\n---\1---\n", text)

    text = re.sub(r"\s*(- \[[ x]\])\s*", r"\n\1 ", text)

    return text.strip()


TITLE_REGEX = re.compile(r"\[AGENT:(?P<intent>\w+)\]\s*(?P<rest>.+)")
SECTION_RE = re.compile(r"---(\w+)---\s*(.*?)(?=---\w+---|\Z)", re.S)

def parse_title(title: str):
    match = TITLE_REGEX.match(title)
    if not match:
        return None, None, title.strip()
    intent = match.group("intent")
    rest = match.group("rest")
    if "::" in rest:
        scope, summary = [s.strip() for s in rest.split("::", 1)]
    else:
        scope, summary = None, rest.strip()
    return intent, scope, summary


def parse_sections(description: str) -> dict:
    return {
        m.group(1).lower(): m.group(2).strip()
        for m in SECTION_RE.finditer(description)
    }


def parse_checklist(text: str) -> list[str]:
    if not text:
        return []
    items = []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("- [ ]"):
            items.append(line.replace("- [ ]", "").strip())
        elif line.startswith("- [x]"):
            items.append(line.replace("- [x]", "").strip())
        elif line.startswith("- ") and line != "-":
            items.append(line[2:].strip())
    return items


def parse_agent_hints(text: str):
    priority = None
    scope = []
    depends_on = []
    branch = None
    if not text:
        return priority, scope, depends_on, branch
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("priority:"):
            priority = line.split(":", 1)[1].strip()
        elif line.startswith("scope:"):
            scope = [s.strip() for s in line.split(":", 1)[1].split(",") if s.strip()]
        elif line.startswith("depends_on:"):
            depends_on = [
                int(x.strip().replace("#", ""))
                for x in line.split(":", 1)[1].split(",")
                if x.strip()
            ]
        elif line.startswith("branch:"):
            branch = line.split(":", 1)[1].strip()
    return priority, scope, depends_on, branch


def get_priority(labels: list[str]) -> str:
    for label in labels:
        if label.startswith("priority::"):
            return label.split("::")[1]
    if "bug" in labels or "security" in labels:
        return "high"
    return "normal"


def normalize(body: dict, action: str, event_id: str, received_at: str) -> Event:
    a = body["object_attributes"]

    description = clean_description(a.get("description") or "")
    intent, scope, summary = parse_title(a["title"])
    sections = parse_sections(description)

    hinted_priority, hinted_scope, depends_on, branch = parse_agent_hints(
        sections.get("agent_hints", "")
    )
    priority = hinted_priority or get_priority(a.get("labels") or [])
    routing_key = f"{intent or 'unknown'}.{priority}.{action}"

    return Event(
        event_id=event_id,
        issue_id=a["id"],
        project=body["project"]["name"],

        title=a["title"],
        intent=intent,
        scope=scope,
        summary=summary,

        description=description,
        context=sections.get("context"),
        acceptance_criteria=parse_checklist(sections.get("acceptance_criteria", "")),
        constraints=sections.get("constraints"),
        non_goals=sections.get("non_goals"),

        priority=priority,
        hinted_scope=hinted_scope,
        depends_on=depends_on,
        branch=branch,

        routing_key=routing_key,
        action=action,

        labels=a.get("labels") or [],
        author=body["user"]["username"],
        url=a["url"],
        created_at=str(a["created_at"]),
        received_at=received_at,
        updated_at=str(a["updated_at"]),
        path_with_namespace=body["project"]["path_with_namespace"],
    )


producer: AIOKafkaProducer | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer

    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode(),
        key_serializer=lambda k: str(k).encode(),
        acks="all",
        enable_idempotence=True,
    )

    for attempt in range(10):
        try:
            await producer.start()
            logger.info("Kafka producer connected")
            break
        except Exception:
            logger.warning(f"Kafka attempt {attempt + 1}/10 failed, retrying...")
            await asyncio.sleep(5)
    else:
        raise RuntimeError("Kafka unavailable")

    yield
    await producer.stop()


async def publish(body: dict, action: str):
    event_id = str(uuid.uuid4())
    received_at = datetime.now(timezone.utc).isoformat()

    raw = RawEvent(
        event_id=event_id,
        received_at=received_at,
        action=action,
        payload=body,
    )
    await producer.send_and_wait(
        "events.raw",
        value=raw.model_dump(mode="json"),
        key=str(body["object_attributes"]["id"]),
    )

    try:
        event = normalize(body, action, event_id, received_at)
        await producer.send_and_wait(
            "events.normalized",
            value=event.model_dump(mode="json"),
            key=str(event.issue_id),
        )
        logger.info(
            f"[{event.routing_key}] Issue #{event.issue_id} ({action}) → {event.summary}"
        )
    except Exception as e:
        logger.exception(
            f"Normalization failed for event_id={event_id} "
            f"issue_id={body['object_attributes'].get('id')} — raw event preserved"
        )
        await producer.send_and_wait(
            "events.failed",
            value={
                "event_id":      event_id,
                "issue_id":      body["object_attributes"].get("id"),
                "action":        action,
                "error":         str(e),
                "received_at":   received_at,
                "raw_topic_key": str(body["object_attributes"].get("id")),
            },
            key=str(body["object_attributes"].get("id")),
        )


app = FastAPI(lifespan=lifespan)


@app.post("/webhook/gitlab", status_code=202)
async def gitlab_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_gitlab_token: str = Header(...),
):
    logger.info(f"POST /webhook/gitlab received from {request.client.host}")
    try:
        raw_body = await asyncio.wait_for(request.body(), timeout=5.0)
        logger.info(f"Body received, length={len(raw_body)}")
    except asyncio.TimeoutError:
        logger.error("Timed out reading request body")
        raise HTTPException(status_code=408, detail="Request timeout")
    if not hmac.compare_digest(x_gitlab_token, settings.gitlab_secret_token):
        raise HTTPException(status_code=401, detail="Invalid token")

    body = json.loads(raw_body)

    if body.get("object_kind") != "issue":
        return {"status": "ignored", "reason": "not an issue"}

    action = body.get("object_attributes", {}).get("action")
    if action not in TRACKED_ACTIONS:
        return {"status": "ignored", "reason": f"untracked action: {action}"}

    background_tasks.add_task(publish, body, action)

    return {"status": "accepted", "action": action}


@app.get("/health")
async def health():
    return {"status": "ok"}