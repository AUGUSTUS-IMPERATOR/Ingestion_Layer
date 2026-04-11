import asyncio
import json
import logging
import os

import httpx
from aiokafka import AIOKafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("consumer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
# ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", "http://orchestrator:8000/orchestrate/async")
ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", "http://orchestrator:8000/orchestrate")
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

            logger.info(f"Dispatching issue #{issue_id} routing_key={routing_key}")

            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    resp = await client.post(
                        ORCHESTRATOR_URL,
                        json={
                            "ticket": {
                                "intent":               event.get("intent") or "fix",
                                "title":                event.get("title", ""),
                                "description":          event.get("description", ""),
                                "summary":              event.get("summary"),
                                "scope":                event.get("scope"),
                                "acceptance_criteria":  event.get("acceptance_criteria", []),
                                "constraints":          event.get("constraints"),
                                "repo_url":             ( f"https://gitlab.com/{event['path_with_namespace']}.git" if event.get("path_with_namespace") else None ),
                                "issue_id":             event.get("issue_id"),
                                "url":                  event.get("url"),
                                "author":               event.get("author"),
                                "branch":               event.get("branch"),
                                "labels":               event.get("labels", []),
                                "project":              event.get("project"),
                                "priority":             event.get("priority") or "normal",
                            }
                        },
                    )
                    resp.raise_for_status()
                    task = resp.json()
                    logger.info(f"Queued task_id={task['task_id']} for issue #{issue_id}")

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