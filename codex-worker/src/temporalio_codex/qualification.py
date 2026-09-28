import argparse
import asyncio
import json
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from typing import Callable

from temporalio_codex.codex_adapter import CodexAdapter
from temporalio_codex.codex_models import (
    CodexOperation,
    CodexOutcome,
    CodexRole,
)
from temporalio_codex.openai_adapter import OpenAICodexAdapter


@dataclass(frozen=True)
class LiveQualification:
    status: str
    sdk_version: str | None = None
    thread_id: str | None = None
    turn_id: str | None = None
    reason: str = ""


async def run_live_qualification(
    *,
    adapter: CodexAdapter | None = None,
    factory: Callable | None = None,
) -> LiveQualification:
    """Run one opt-in SDK turn and return evidence without exposing secrets."""
    if adapter is None:
        try:
            from openai_codex import AsyncCodex

            sdk_version = version("openai-codex")
        except (ImportError, PackageNotFoundError):
            return LiveQualification(
                status="not_verified",
                reason="openai-codex is not installed",
            )
        adapter = OpenAICodexAdapter(factory or AsyncCodex, sdk_version)
    else:
        sdk_version = None

    try:
        observation = await adapter.execute(
            CodexOperation(
                operation_id="live-qualification-1",
                run_id="live-qualification",
                stage="qualification",
                role=CodexRole.PLANNING,
                repository=".",
                allowed_scope=("read_only",),
                approval_policy="deny_all",
                model="gpt-5-codex",
                effort="low",
                prompt="Return a short qualification response.",
            )
        )
    except Exception:
        return LiveQualification(
            status="not_verified",
            sdk_version=sdk_version,
            reason="live SDK qualification raised an unexpected error",
        )
    observed_sdk_version = (
        observation.capabilities.sdk_version
        if observation.capabilities is not None
        else sdk_version
    )
    if (
        observation.outcome is CodexOutcome.COMPLETED
        and observation.thread_id
        and observation.turn_id
    ):
        return LiveQualification(
            status="verified",
            sdk_version=observed_sdk_version,
            thread_id=observation.thread_id,
            turn_id=observation.turn_id,
            reason="live SDK turn completed",
        )
    return LiveQualification(
        status="not_verified",
        sdk_version=observed_sdk_version,
        thread_id=observation.thread_id,
        turn_id=observation.turn_id,
        reason="live SDK completion and identities were not observed",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the opt-in live Codex SDK qualification"
    )
    parser.parse_args()
    result = asyncio.run(run_live_qualification())
    print(json.dumps(result.__dict__, sort_keys=True))
