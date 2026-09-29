"""Deterministic and opt-in live acceptance seams for arbitrary-flow takeover.

The deterministic harness models the external readbacks that a takeover must
reconcile. It records every action and rejects state that would cause the
Runner to recreate work already represented by authoritative evidence.
"""
from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class TakeoverStatus(StrEnum):
    VERIFIED = "verified"
    BLOCKED = "blocked"
    CLEANUP_PENDING = "cleanup_pending"


class TakeoverFrontier(StrEnum):
    SPEC_DISCOVERED = "spec_discovered"
    SPEC_READY = "spec_ready"
    TICKETS_ADOPTED = "tickets_adopted"
    IMPLEMENTATION_NEEDED = "implementation_needed"
    CANDIDATE_READY = "candidate_ready"
    CHECKS_PENDING = "checks_pending"
    REVIEW_PENDING = "review_pending"
    MERGE_PENDING = "merge_pending"
    CLOSURE_PENDING = "closure_pending"
    COMPLETED = "completed"
    BLOCKED = "blocked"


@dataclass
class SpecProgress:
    key: str
    blocked_by: tuple[str, ...] = ()
    tickets_ready: bool = False
    candidate: bool = False
    candidate_receipt: bool = False
    review: bool = False
    review_receipt: bool = False
    pull_request: bool = False
    checks_ready: bool = False
    merged: bool = False
    target_synced: bool = False
    issue_closed: bool = False
    cleanup: bool = False
    local_receipts: bool = True
    merge_timeout_once: bool = False
    cleanup_failures: int = 0

    def complete(self) -> bool:
        return all((
            self.tickets_ready,
            self.candidate,
            self.candidate_receipt,
            self.review,
            self.review_receipt,
            self.pull_request,
            self.checks_ready,
            self.merged,
            self.target_synced,
            self.issue_closed,
            self.cleanup,
            self.local_receipts,
        ))

    def frontier(self) -> str:
        if self.complete():
            return TakeoverFrontier.COMPLETED
        if not self.tickets_ready:
            return TakeoverFrontier.SPEC_READY
        if not self.candidate:
            return TakeoverFrontier.IMPLEMENTATION_NEEDED
        if not self.review or not self.review_receipt:
            return TakeoverFrontier.REVIEW_PENDING
        if not self.pull_request:
            return TakeoverFrontier.CANDIDATE_READY
        if not self.checks_ready:
            return TakeoverFrontier.CHECKS_PENDING
        if not self.merged:
            return TakeoverFrontier.MERGE_PENDING
        if not self.issue_closed or not self.target_synced:
            return TakeoverFrontier.CLOSURE_PENDING
        if not self.cleanup or not self.local_receipts:
            return TakeoverFrontier.CLOSURE_PENDING
        return TakeoverFrontier.BLOCKED

    def as_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "blocked_by": list(self.blocked_by),
            "tickets_ready": self.tickets_ready,
            "candidate": self.candidate,
            "candidate_receipt": self.candidate_receipt,
            "review": self.review,
            "review_receipt": self.review_receipt,
            "pull_request": self.pull_request,
            "checks_ready": self.checks_ready,
            "merged": self.merged,
            "target_synced": self.target_synced,
            "issue_closed": self.issue_closed,
            "cleanup": self.cleanup,
            "local_receipts": self.local_receipts,
            "merge_timeout_once": self.merge_timeout_once,
            "cleanup_failures": self.cleanup_failures,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SpecProgress":
        return cls(
            key=str(value["key"]),
            blocked_by=tuple(str(item) for item in value.get("blocked_by", ())),
            **{
                name: value.get(name, default)
                for name, default in (
                    ("tickets_ready", False), ("candidate", False),
                    ("candidate_receipt", False), ("review", False),
                    ("review_receipt", False), ("pull_request", False),
                    ("checks_ready", False), ("merged", False),
                    ("target_synced", False), ("issue_closed", False),
                    ("cleanup", False), ("local_receipts", True),
                    ("merge_timeout_once", False), ("cleanup_failures", 0),
                )
            },
        )


@dataclass
class TakeoverState:
    scenario: str
    source_revision: str
    grill_complete: bool
    spec_graph_complete: bool
    expected_specs: tuple[str, ...]
    specs: dict[str, SpecProgress] = field(default_factory=dict)
    blockers: tuple[str, ...] = ()
    actions: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    implementation_calls: int = 0
    merge_calls: int = 0
    cleanup_calls: int = 0
    cleanup_retry_calls: int = 0
    source_changed: bool = False

    def serialize(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "source_revision": self.source_revision,
            "grill_complete": self.grill_complete,
            "spec_graph_complete": self.spec_graph_complete,
            "expected_specs": list(self.expected_specs),
            "specs": {key: value.as_dict() for key, value in self.specs.items()},
            "blockers": list(self.blockers),
            "actions": list(self.actions),
            "skipped": list(self.skipped),
            "implementation_calls": self.implementation_calls,
            "merge_calls": self.merge_calls,
            "cleanup_calls": self.cleanup_calls,
            "cleanup_retry_calls": self.cleanup_retry_calls,
            "source_changed": self.source_changed,
        }

    @classmethod
    def deserialize(cls, value: dict[str, Any]) -> "TakeoverState":
        return cls(
            scenario=str(value["scenario"]),
            source_revision=str(value["source_revision"]),
            grill_complete=bool(value["grill_complete"]),
            spec_graph_complete=bool(value["spec_graph_complete"]),
            expected_specs=tuple(str(item) for item in value["expected_specs"]),
            specs={key: SpecProgress.from_dict(item) for key, item in value.get("specs", {}).items()},
            blockers=tuple(str(item) for item in value.get("blockers", ())),
            actions=list(value.get("actions", ())),
            skipped=list(value.get("skipped", ())),
            implementation_calls=int(value.get("implementation_calls", 0)),
            merge_calls=int(value.get("merge_calls", 0)),
            cleanup_calls=int(value.get("cleanup_calls", 0)),
            cleanup_retry_calls=int(value.get("cleanup_retry_calls", 0)),
            source_changed=bool(value.get("source_changed", False)),
        )


@dataclass(frozen=True)
class TakeoverRunResult:
    status: TakeoverStatus
    state: TakeoverState
    reason: str = ""
    snapshot_digest: str = ""


def _digest(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _progress(**overrides: Any) -> SpecProgress:
    return SpecProgress(**overrides)


def scenario_state(name: str) -> TakeoverState:
    """Create one named matrix fixture without touching GitHub or a checkout."""
    specs = {
        "S1": _progress(key="S1"),
        "S2": _progress(key="S2", blocked_by=("S1",)),
    }
    state = TakeoverState(
        scenario=name,
        source_revision="source-v1",
        grill_complete=False,
        spec_graph_complete=False,
        expected_specs=("S1", "S2"),
    )
    if name == "fresh":
        state.specs = {}
    elif name == "existing-grill":
        state.grill_complete = True
        state.specs = {}
    elif name == "complete-spec":
        state.grill_complete = True
        state.spec_graph_complete = True
        state.specs = {key: copy.deepcopy(value) for key, value in specs.items()}
    elif name == "partial-spec":
        state.grill_complete = True
        state.specs = {"S1": specs["S1"]}
    elif name == "tickets-ready":
        state.grill_complete = True
        state.spec_graph_complete = True
        for progress in specs.values():
            progress.tickets_ready = True
        state.specs = specs
    elif name == "mixed-completed":
        state.grill_complete = True
        state.spec_graph_complete = True
        for field in ("tickets_ready", "candidate", "candidate_receipt", "review", "review_receipt", "pull_request", "checks_ready", "merged", "target_synced", "issue_closed", "cleanup"):
            setattr(specs["S1"], field, True)
        specs["S2"].tickets_ready = True
        state.specs = specs
    elif name == "candidate-no-checks":
        state.grill_complete = True
        state.spec_graph_complete = True
        specs["S1"].tickets_ready = True
        specs["S1"].candidate = True
        specs["S1"].candidate_receipt = True
        specs["S1"].review = True
        specs["S1"].review_receipt = True
        specs["S1"].pull_request = True
        state.specs = {"S1": specs["S1"]}
        state.expected_specs = ("S1",)
    elif name == "remote-evidence-no-receipts":
        state.grill_complete = True
        state.spec_graph_complete = True
        progress = specs["S1"]
        for field in ("tickets_ready", "candidate", "candidate_receipt", "review", "review_receipt", "pull_request", "checks_ready", "merged", "target_synced", "issue_closed", "cleanup"):
            setattr(progress, field, True)
        progress.local_receipts = False
        state.specs = {"S1": progress}
        state.expected_specs = ("S1",)
    elif name == "merged-no-close":
        state.grill_complete = True
        state.spec_graph_complete = True
        progress = specs["S1"]
        for field in ("tickets_ready", "candidate", "candidate_receipt", "review", "review_receipt", "pull_request", "checks_ready", "merged", "target_synced"):
            setattr(progress, field, True)
        state.specs = {"S1": progress}
        state.expected_specs = ("S1",)
    elif name == "closed-no-merge":
        state.grill_complete = True
        state.spec_graph_complete = True
        progress = specs["S1"]
        for field in ("tickets_ready", "candidate", "candidate_receipt", "review", "review_receipt", "pull_request", "checks_ready", "issue_closed"):
            setattr(progress, field, True)
        state.specs = {"S1": progress}
        state.expected_specs = ("S1",)
    elif name == "merge-timeout":
        state = scenario_state("closed-no-merge")
        state.scenario = name
        state.specs["S1"].merge_timeout_once = True
    elif name == "dependency-order":
        state = scenario_state("tickets-ready")
    elif name == "cleanup-retry":
        state = scenario_state("merged-no-close")
        state.scenario = name
        progress = state.specs["S1"]
        progress.issue_closed = True
        progress.cleanup_failures = 1
    elif name in {"snapshot-change", "relation-conflict", "duplicate-issue", "branch-drift", "dirty-worktree", "remote-divergence", "scope-violation"}:
        state = scenario_state("tickets-ready")
        state.scenario = name
        state.blockers = (name,)
    else:
        raise ValueError(f"unknown takeover scenario: {name}")
    return state


def snapshot_for(state: TakeoverState) -> dict[str, Any]:
    external = {
        "scenario": state.scenario,
        "source_revision": state.source_revision,
        "expected_specs": list(state.expected_specs),
        "blockers": list(state.blockers),
    }
    return {"schema_version": "tc083-takeover-snapshot/v1", "source": external, "digest": _digest(external)}


def _ready_specs(state: TakeoverState) -> list[SpecProgress]:
    completed = {key for key, progress in state.specs.items() if progress.complete()}
    return [
        state.specs[key]
        for key in state.expected_specs
        if key in state.specs
        and not state.specs[key].complete()
        and set(state.specs[key].blocked_by) <= completed
    ]


def _record_skip(state: TakeoverState, key: str) -> None:
    if key not in state.skipped:
        state.skipped.append(key)


def _next_action(state: TakeoverState) -> tuple[str, SpecProgress | None]:
    if state.blockers:
        return "blocked", None
    if not state.grill_complete:
        return "grill", None
    if not state.spec_graph_complete:
        return "to-spec", None
    for key in state.expected_specs:
        progress = state.specs.get(key)
        if progress is None:
            return "to-spec", None
        if progress.complete():
            _record_skip(state, key)
    ready = _ready_specs(state)
    if not ready:
        return "completed", None
    progress = ready[0]
    if not progress.tickets_ready:
        return "to-tickets", progress
    if not progress.candidate:
        return "implement", progress
    if not progress.candidate_receipt or not progress.review or not progress.review_receipt:
        return "review", progress
    if not progress.pull_request:
        return "pull-request", progress
    if not progress.checks_ready:
        return "checks", progress
    if not progress.merged:
        return "merge", progress
    if not progress.issue_closed:
        return "close", progress
    if not progress.cleanup:
        return "cleanup", progress
    if not progress.local_receipts:
        return "reconcile-receipts", progress
    return "blocked", progress


def _apply_action(state: TakeoverState, action: str, progress: SpecProgress | None) -> tuple[TakeoverStatus | None, str]:
    state.actions.append(action if progress is None else f"{action}:{progress.key}")
    if action == "grill":
        state.grill_complete = True
    elif action == "to-spec":
        state.spec_graph_complete = True
        for key in state.expected_specs:
            state.specs.setdefault(key, SpecProgress(key=key, blocked_by=("S1",) if key == "S2" else ()))
    elif action == "to-tickets":
        assert progress is not None
        progress.tickets_ready = True
    elif action == "implement":
        assert progress is not None
        state.implementation_calls += 1
        progress.candidate = True
        progress.candidate_receipt = True
    elif action == "review":
        assert progress is not None
        progress.review = True
        progress.review_receipt = True
    elif action == "pull-request":
        assert progress is not None
        progress.pull_request = True
    elif action == "checks":
        assert progress is not None
        progress.checks_ready = True
    elif action == "merge":
        assert progress is not None
        state.merge_calls += 1
        if progress.merge_timeout_once:
            progress.merge_timeout_once = False
            progress.merged = True
            progress.target_synced = True
            return TakeoverStatus.BLOCKED, "merge outcome unknown; authoritative readback required"
        progress.merged = True
        progress.target_synced = True
    elif action == "close":
        assert progress is not None
        progress.issue_closed = True
    elif action == "cleanup":
        assert progress is not None
        state.cleanup_calls += 1
        if progress.cleanup_failures:
            progress.cleanup_failures -= 1
            return TakeoverStatus.CLEANUP_PENDING, "cleanup readback is pending"
        progress.cleanup = True
    elif action == "reconcile-receipts":
        assert progress is not None
        state.cleanup_retry_calls += 1
        progress.local_receipts = True
    return None, ""


class TakeoverHarness:
    """A restartable acceptance harness for one arbitrary-state takeover."""

    def __init__(self, state: TakeoverState):
        self.state = state

    @classmethod
    def for_scenario(cls, name: str) -> "TakeoverHarness":
        return cls(scenario_state(name))

    @classmethod
    def from_serialized(cls, value: dict[str, Any]) -> "TakeoverHarness":
        return cls(TakeoverState.deserialize(value))

    def snapshot_digest(self) -> str:
        return str(snapshot_for(self.state)["digest"])

    def mutate_source(self) -> None:
        self.state.source_changed = True
        self.state.source_revision = "source-v2"

    def run(self, *, max_actions: int | None = None, expected_snapshot_digest: str | None = None) -> TakeoverRunResult:
        if expected_snapshot_digest is not None and expected_snapshot_digest != self.snapshot_digest():
            return TakeoverRunResult(TakeoverStatus.BLOCKED, self.state, "takeover source snapshot changed", self.snapshot_digest())
        if self.state.source_changed:
            return TakeoverRunResult(TakeoverStatus.BLOCKED, self.state, "takeover source snapshot changed", self.snapshot_digest())
        if self.state.blockers:
            return TakeoverRunResult(TakeoverStatus.BLOCKED, self.state, f"discovery blocker: {self.state.blockers[0]}", self.snapshot_digest())
        executed = 0
        while max_actions is None or executed < max_actions:
            action, progress = _next_action(self.state)
            if action == "completed":
                return TakeoverRunResult(TakeoverStatus.VERIFIED, self.state, snapshot_digest=self.snapshot_digest())
            if action == "blocked":
                return TakeoverRunResult(TakeoverStatus.BLOCKED, self.state, "no safe takeover frontier", self.snapshot_digest())
            status, reason = _apply_action(self.state, action, progress)
            executed += 1
            if status is not None:
                return TakeoverRunResult(status, self.state, reason, self.snapshot_digest())
        return TakeoverRunResult(TakeoverStatus.BLOCKED, self.state, "process exited at a durable frontier", self.snapshot_digest())

    def serialized_state(self) -> dict[str, Any]:
        return self.state.serialize()


def run_takeover_matrix(names: tuple[str, ...] | None = None) -> tuple[TakeoverRunResult, ...]:
    selected = names or (
        "fresh", "existing-grill", "complete-spec", "partial-spec", "tickets-ready",
        "mixed-completed", "candidate-no-checks", "remote-evidence-no-receipts",
        "merged-no-close", "closed-no-merge", "merge-timeout", "dependency-order",
        "cleanup-retry",
    )
    results: list[TakeoverRunResult] = []
    for name in selected:
        harness = TakeoverHarness.for_scenario(name)
        result = harness.run()
        if result.status in {TakeoverStatus.BLOCKED, TakeoverStatus.CLEANUP_PENDING} and name in {"merge-timeout", "cleanup-retry"}:
            result = harness.run(expected_snapshot_digest=result.snapshot_digest)
        results.append(result)
    return tuple(results)
