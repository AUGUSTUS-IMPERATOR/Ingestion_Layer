import asyncio
import json
import logging
import os
import uuid
from datetime import datetime

import httpx
from aiokafka import AIOKafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("consumer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", "http://orchestrator:8000/ticket/sync")
TRACKED_ACTIONS = {"open", "update"}

async def consume():
    consumer = AIOKafkaConsumer(
        "events.normalized",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="orchestrator-consumer",
        value_deserializer=lambda v: json.loads(v.decode()),
        auto_offset_reset="earliest",
    )

    for attempt in range(10):
        try:
            await consumer.start()
            logger.info("Consumer connected to Kafka")
            break
        except Exception:
            logger.warning(f"Kafka not ready, attempt {attempt + 1}/10, retrying in 5s...")
            await asyncio.sleep(5)
    else:
        raise RuntimeError("Could not connect to Kafka after 10 attempts")

    try:
        async for msg in consumer:
            event = msg.value
            issue_id = event.get("issue_id")
            action = event.get("action")
            routing_key = event.get("routing_key", "unknown")

            if action not in TRACKED_ACTIONS:
                logger.info(f"Skipping issue #{issue_id} action={action}")
                continue

            # ✅ Extract workspace_path inside the loop
            namespace = event.get("path_with_namespace")
            if namespace:
                repo_name = namespace.split("/")[-1]
                workspace_path = f"/workspaces/ticket-{issue_id}"
            else:
                workspace_path = None
                logger.warning(f"Issue #{issue_id} has no path_with_namespace")

            logger.info(f"Dispatching issue #{issue_id} routing_key={routing_key}")

            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    resp = await client.post(
                        ORCHESTRATOR_URL,
                        json={
                            "event_id": str(uuid.uuid4()),
                            "issue_id": issue_id,
                            "project": event.get("project"),
                            "title": event.get("title", ""),
                            "intent": event.get("intent") or "fix",
                            "scope": event.get("scope"),
                            "summary": event.get("summary") or event.get("title", "")[:100],
                            "description": event.get("description", ""),
                            "context": event.get("context"),
                            "acceptance_criteria": event.get("acceptance_criteria", []),
                            "constraints": event.get("constraints"),
                            "non_goals": event.get("non_goals"),
                            "priority": event.get("priority") or "normal",
                            "hinted_scope": event.get("hinted_scope", []),
                            "depends_on": event.get("depends_on", []),
                            "branch": event.get("branch"),
                            "routing_key": event.get("routing_key", f"{event.get('intent','fix')}.{event.get('priority','normal')}.{action}"),
                            "action": action,
                            "labels": event.get("labels", []),
                            "author": event.get("author"),
                            "url": event.get("url"),
                            "created_at": datetime.utcnow().isoformat(),
                            "received_at": datetime.utcnow().isoformat(),
                            "updated_at": None,
                            "workspace_path": workspace_path,
                        },
                    )
                    resp.raise_for_status()
                    result = resp.json()
                    # ✅ Changed from task['task_id'] to result.get('ticket_id')
                    logger.info(f"Queued ticket_id={result.get('ticket_id')} for issue #{issue_id}")

            except httpx.HTTPStatusError as e:
                logger.error(f"Orchestrator rejected issue #{issue_id}: {e.response.status_code} {e.response.text}")
            except httpx.RequestError as e:
                logger.error(f"Could not reach orchestrator for issue #{issue_id}: {e}")
            except Exception as e:
                logger.exception(f"Unexpected error dispatching issue #{issue_id}: {e}")

    finally:
        await consumer.stop()
        logger.info("Consumer stopped")


if __name__ == "__main__":
    asyncio.run(consume())