import asyncio
import json
import logging
import os
import shutil
from pathlib import Path

from aiokafka import AIOKafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("repo-syncer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
WORKDIR = Path(os.getenv("REPO_WORKDIR", "/repos"))
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN", "")

_repo_locks: dict[str, asyncio.Lock] = {}


def _lock_for(namespace: str) -> asyncio.Lock:
    if namespace not in _repo_locks:
        _repo_locks[namespace] = asyncio.Lock()
    return _repo_locks[namespace]


def _authenticated_url(repo_url: str) -> str:
    if GITLAB_TOKEN and repo_url.startswith("https://"):
        return repo_url.replace("https://", f"https://oauth2:{GITLAB_TOKEN}@", 1)
    return repo_url


async def _run(cmd: list[str], cwd: Path | None = None) -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(cwd) if cwd else None,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    return proc.returncode, stdout.decode().strip(), stderr.decode().strip()


async def sync_repo(namespace: str, repo_url: str, branch: str | None) -> None:
    repo_name = namespace.split("/")[-1]
    dest = WORKDIR / repo_name
    auth_url = _authenticated_url(repo_url)

    async with _lock_for(namespace):
        if not dest.exists():
            logger.info(f"Cloning {namespace} → {dest}")
            dest.parent.mkdir(parents=True, exist_ok=True)
            rc, _, err = await _run(["git", "clone", "--filter=blob:none", auth_url, str(dest)])
            if rc != 0:
                logger.error(f"Clone failed for {namespace}: {err}")
                shutil.rmtree(dest, ignore_errors=True)
                return
            logger.info(f"Cloned {namespace} successfully")
        else:
            logger.info(f"Fetching updates for {namespace}")
            rc, _, err = await _run(["git", "fetch", "--all", "--prune"], cwd=dest)
            if rc != 0:
                logger.warning(f"Fetch failed for {namespace}: {err}")

        rc, default_branch, _ = await _run(
            ["git", "rev-parse", "--abbrev-ref", "origin/HEAD"], cwd=dest
        )
        if rc != 0 or not default_branch:
            default_branch = "origin/main"

        checkout_ref = f"origin/{branch}" if branch else default_branch
        rc, _, err = await _run(["git", "reset", "--hard", checkout_ref], cwd=dest)
        if rc != 0:
            logger.warning(f"Reset to {checkout_ref} failed for {namespace}: {err}")
        else:
            rc2, sha, _ = await _run(["git", "rev-parse", "--short", "HEAD"], cwd=dest)
            logger.info(f"Repo {namespace} is at {sha} ({checkout_ref})")


async def consume() -> None:
    WORKDIR.mkdir(parents=True, exist_ok=True)

    consumer = AIOKafkaConsumer(
        "events.normalized",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="repo-syncer",
        value_deserializer=lambda v: json.loads(v.decode()),
        auto_offset_reset="earliest",
    )

    for attempt in range(10):
        try:
            await consumer.start()
            logger.info("Repo-syncer connected to Kafka")
            break
        except Exception:
            logger.warning(f"Kafka not ready, attempt {attempt + 1}/10, retrying in 5s…")
            await asyncio.sleep(5)
    else:
        raise RuntimeError("Could not connect to Kafka after 10 attempts")

    try:
        async for msg in consumer:
            event = msg.value
            issue_id = event.get("issue_id")
            action = event.get("action")
            namespace = event.get("path_with_namespace")
            branch = event.get("branch")

            if action not in {"open", "update"}:
                continue

            if not namespace:
                logger.warning(f"Issue #{issue_id} has no path_with_namespace, skipping")
                continue

            repo_url = f"https://gitlab.com/{namespace}.git"
            logger.info(f"Issue #{issue_id} ({action}) — syncing repo {namespace}")

            asyncio.create_task(
                sync_repo(namespace, repo_url, branch),
                name=f"sync-{namespace}-{issue_id}",
            )

    finally:
        await consumer.stop()
        logger.info("Repo-syncer stopped")


if __name__ == "__main__":
    asyncio.run(consume())