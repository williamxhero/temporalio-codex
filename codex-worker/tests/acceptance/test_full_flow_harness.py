from __future__ import annotations

import json

from temporalio_codex.full_flow_harness import (
    DeterministicFullFlowBoundary,
    HarnessRunInput,
    HarnessStatus,
    ManifestStore,
    ResourceRecord,
    _authorized_config_repository,
    _run_handoff,
    _write_live_summary,
    default_manifest_path,
    marker_matches,
    retry_cleanup,
    run_deterministic_harness,
    run_live_harness,
    validate_live_manifest,
    validate_runner_evidence,
)


async def test_deterministic_harness_writes_manifest_before_external_writes(tmp_path) -> None:
    manifest_path = tmp_path / "run.json"
    boundary = DeterministicFullFlowBoundary("placeholder")
    result = await run_deterministic_harness(
        HarnessRunInput(repository="owner/repo"),
        manifest_path=manifest_path,
        boundary=boundary,
    )

    assert result.status is HarnessStatus.VERIFIED
    assert manifest_path.is_file()
    manifest = ManifestStore(manifest_path).read()
    assert manifest.status is HarnessStatus.VERIFIED
    assert manifest.resources
    assert len(boundary.write_calls) == len(boundary.cleanup_calls)
    assert all(marker_matches(resource.marker, manifest.marker) for resource in manifest.resources)


async def test_live_summary_readback_contains_the_verified_flow_evidence(tmp_path) -> None:
    manifest_path = tmp_path / "run.json"
    result = await run_deterministic_harness(
        HarnessRunInput(repository="owner/repo"),
        manifest_path=manifest_path,
        boundary=DeterministicFullFlowBoundary("placeholder"),
    )

    summary_path = _write_live_summary(tmp_path / "summary", result.manifest, ("S1", "S2"))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))

    assert summary == {
        "schema_version": "tc083-live-summary/v1",
        "run_id": result.manifest.run_id,
        "marker": result.manifest.marker,
        "runner_repository": "owner/repo",
        "spec_keys": ["S1", "S2"],
        "evidence_refs": [item.reference for item in result.manifest.evidence],
        "resource_refs": [item.identity for item in result.manifest.resources],
        "status": "verified",
        "limitations": [],
    }


async def test_deterministic_harness_adopts_lost_write_without_duplicate_resource(tmp_path) -> None:
    manifest_path = tmp_path / "run.json"
    boundary = DeterministicFullFlowBoundary("placeholder", lost_writes={"never-known"})
    original_write = boundary.write

    async def lose_first_write(phase, operation_id, resource_type):
        if not boundary.write_calls:
            boundary.lost_writes.add(operation_id)
        return await original_write(phase, operation_id, resource_type)

    boundary.write = lose_first_write
    result = await run_deterministic_harness(
        HarnessRunInput(repository="owner/repo", phases=("intake",)),
        manifest_path=manifest_path,
        boundary=boundary,
    )

    assert result.status is HarnessStatus.VERIFIED
    assert len(boundary.resources) == 1
    assert any(
        dict(item.details).get("adopted_after_lost_response") == "true"
        for item in result.manifest.evidence
    )


async def test_cleanup_failure_is_durable_and_does_not_claim_success(tmp_path) -> None:
    manifest_path = tmp_path / "run.json"
    boundary = DeterministicFullFlowBoundary("placeholder", cleanup_failures={"intake-1"})
    result = await run_deterministic_harness(
        HarnessRunInput(repository="owner/repo", phases=("intake",)),
        manifest_path=manifest_path,
        boundary=boundary,
    )

    assert result.status is HarnessStatus.CLEANUP_PENDING
    assert ManifestStore(manifest_path).read().failure_reason


async def test_cleanup_retry_reuses_manifest_and_does_not_rerun_writes(tmp_path) -> None:
    manifest_path = tmp_path / "run.json"
    boundary = DeterministicFullFlowBoundary("placeholder", cleanup_failures={"intake-1"})
    first = await run_deterministic_harness(
        HarnessRunInput(repository="owner/repo", phases=("intake",)),
        manifest_path=manifest_path,
        boundary=boundary,
    )
    assert first.status is HarnessStatus.CLEANUP_PENDING
    write_count = len(boundary.write_calls)
    boundary.cleanup_failures.clear()

    second = await retry_cleanup(manifest_path, boundary=boundary)

    assert second.status is HarnessStatus.VERIFIED
    assert len(boundary.write_calls) == write_count
    assert len(boundary.cleanup_calls) == 1
    assert ManifestStore(manifest_path).read().resources[0].state == "closed"


async def test_stale_cleanup_readback_is_not_success(tmp_path) -> None:
    manifest_path = tmp_path / "run.json"
    boundary = DeterministicFullFlowBoundary("placeholder", stale_reads={"tc083-stale:cleanup"})

    async def stale_cleanup_write(resource):
        boundary.resources[resource.operation_id] = resource
        boundary.stale_reads.add(resource.operation_id)

    boundary.cleanup = stale_cleanup_write
    result = await run_deterministic_harness(
        HarnessRunInput(repository="owner/repo", phases=("cleanup",)),
        manifest_path=manifest_path,
        boundary=boundary,
    )

    assert result.status is HarnessStatus.CLEANUP_PENDING
    assert "cleanup readback" in result.reason


async def test_cleanup_rejects_resource_outside_run_marker(tmp_path) -> None:
    manifest_path = tmp_path / "run.json"
    boundary = DeterministicFullFlowBoundary("placeholder")
    original_write = boundary.write

    async def foreign_write(phase, operation_id, resource_type):
        resource = await original_write(phase, operation_id, resource_type)
        return ResourceRecord(resource.resource_type, resource.identity, "<!-- tc083:foreign -->", resource.operation_id)

    boundary.write = foreign_write
    result = await run_deterministic_harness(
        HarnessRunInput(repository="owner/repo", phases=("intake",)),
        manifest_path=manifest_path,
        boundary=boundary,
    )

    assert result.status is HarnessStatus.FAILED
    assert "outside this run marker" in result.reason


async def test_live_harness_is_not_verified_without_explicit_opt_in(tmp_path) -> None:
    manifest_path = tmp_path / "live.json"
    result = await run_live_harness(
        HarnessRunInput(repository="owner/repo"),
        manifest_path=manifest_path,
        opt_in=False,
    )

    assert result.status is HarnessStatus.NOT_VERIFIED
    assert "TC083_RUN_LIVE" in result.reason
    assert ManifestStore(manifest_path).read().status is HarnessStatus.NOT_VERIFIED


def test_live_validation_rejects_installed_or_deterministic_evidence() -> None:
    payload = {
        "schema_version": "tc083-harness/v1",
        "run_id": "run-1",
        "marker": "<!-- tc083:run-1 -->",
        "build_id": "build-1",
        "repository": "owner/repo",
        "scenario": "live",
        "command": "live",
        "operating_system": "Windows",
        "status": "verified",
        "phase": "complete",
        "capabilities": [],
        "resources": [
            {
                "resource_type": "issue",
                "identity": "issue-1",
                "marker": "<!-- tc083:run-1 -->",
                "operation_id": "run-1:issue",
                "state": "closed",
            }
        ],
        "evidence": [
            {
                "reference": "issue-1",
                "kind": "installed",
                "status": "verified",
                "operation_id": "run-1:issue",
                "phase": "issue",
                "details": [],
            }
        ],
        "cleanup_plan": [],
        "cleanup_attempted": True,
        "failure_reason": "",
    }
    from temporalio_codex.full_flow_harness import HarnessManifest

    errors = validate_live_manifest(HarnessManifest.from_json(json.dumps(payload)))
    assert any("cannot qualify verified live status" in error for error in errors)


def test_runner_evidence_rejects_wrong_pr_head_and_failed_checks(tmp_path) -> None:
    run_id = "runner-1"
    artifact_directory = tmp_path / "artifacts" / run_id
    artifact_directory.mkdir(parents=True)
    (artifact_directory / "spec-plan.json").write_text(
        json.dumps({"outcome": "planned", "specs": [{"key": "S1"}]}), encoding="utf-8"
    )
    (artifact_directory / "ticket-plan-S1.json").write_text(
        json.dumps({"outcome": "planned", "spec_key": "S1"}), encoding="utf-8"
    )
    candidate_sha = "a" * 40
    (artifact_directory / "candidate-S1.json").write_text(
        json.dumps({"outcome": "verified", "candidate_sha": candidate_sha}), encoding="utf-8"
    )
    (artifact_directory / f"review-S1-{candidate_sha[:12]}.json").write_text(
        json.dumps({"approved": True, "candidate_sha": candidate_sha}), encoding="utf-8"
    )
    (artifact_directory / "delivery-S1.json").write_text(
        json.dumps(
            {
                "state": "github_completed",
                "checks": {"candidate_sha": candidate_sha, "ready": False},
                "pr": {"number": 12, "candidate_sha": "b" * 40},
                "merge": {"merged": False},
                "issue_closure": {"complete": False},
            }
        ),
        encoding="utf-8",
    )
    status = {
        "run": {"run_id": run_id, "state": "completed"},
        "workers": [
            {"worker_id": f"codex_sdk:{run_id}:codex_{role}:S1", "external_thread_id": f"thread-{role}", "external_turn_id": f"turn-{role}", "backend_kind": "codex_sdk", "model": "gpt-test", "approval_mode": "deny_all", "state": "completed"}
            for role in ("planning", "implementation", "review")
        ],
    }

    errors, evidence, resources, specs = validate_runner_evidence(
        status,
        artifact_directory=artifact_directory,
        runner_run_id=run_id,
        repository="owner/repo",
        marker="<!-- tc083:manifest-1 -->",
    )

    assert specs == ("S1",)
    assert not evidence
    assert not resources
    assert any("checks S1" in error for error in errors)
    assert any("PR S1" in error for error in errors)


async def test_live_entry_requires_explicit_authorized_inputs(tmp_path) -> None:
    result = await run_live_harness(
        HarnessRunInput(repository="owner/repo"),
        manifest_path=tmp_path / "live.json",
        opt_in=True,
    )

    assert result.status is HarnessStatus.NOT_VERIFIED
    assert "authorized-brief" in result.reason or "required live capabilities" in result.reason


def test_live_config_must_authorize_the_requested_repository(tmp_path) -> None:
    config_path = tmp_path / "runner.json"
    config_path.write_text(json.dumps({"github": {"repository": "other/repo"}}), encoding="utf-8")

    authorized, detail = _authorized_config_repository(
        HarnessRunInput(repository="owner/repo", config_path=config_path)
    )

    assert not authorized
    assert "expected 'owner/repo'" in detail


def test_live_handoff_requires_the_run_marker_in_the_brief(tmp_path) -> None:
    brief_path = tmp_path / "brief.txt"
    brief_path.write_text("authorized requirement", encoding="utf-8")
    config_path = tmp_path / "runner.json"
    config_path.write_text("{}", encoding="utf-8")

    try:
        _run_handoff(
            tmp_path / "skill",
            input=HarnessRunInput(repository="owner/repo", brief_path=brief_path, config_path=config_path),
            run_id="live-1",
            control_root=tmp_path / "control",
        )
    except ValueError as error:
        assert "run marker" in str(error)
    else:
        raise AssertionError("brief without the run marker must not launch")


def _write_worker_receipts(artifact_directory, workers) -> None:
    filenames = {
        "planning": "codex_planning-planning.json",
        "implementation": "implementation-S1.json",
        "review": "review-worker-S1-aaaaaaaaaaaa.json",
    }
    for role, filename in filenames.items():
        worker = next(item for item in workers if f"codex_{role}" in item["worker_id"])
        (artifact_directory / filename).write_text(
            json.dumps(
                {
                    "status": "completed",
                    "error": None,
                    "thread_id": worker["external_thread_id"],
                    "turn_id": worker["external_turn_id"],
                    "approval_mode": "deny_all",
                    "sdk_version": "0.155.1",
                }
            ),
            encoding="utf-8",
        )


def test_runner_evidence_accepts_complete_live_receipts(tmp_path) -> None:
    run_id = "runner-complete"
    marker = "<!-- tc083:manifest-complete -->"
    artifact_directory = tmp_path / "artifacts" / run_id
    artifact_directory.mkdir(parents=True)
    (artifact_directory / "spec-plan.json").write_text(
        json.dumps({"outcome": "planned", "specs": [{"key": "S1"}]}), encoding="utf-8"
    )
    (artifact_directory / "ticket-plan-S1.json").write_text(
        json.dumps({"outcome": "planned", "spec_key": "S1"}), encoding="utf-8"
    )
    candidate_sha = "a" * 40
    merge_sha = "b" * 40
    (artifact_directory / "candidate-S1.json").write_text(
        json.dumps({"outcome": "verified", "candidate_sha": candidate_sha}), encoding="utf-8"
    )
    (artifact_directory / f"review-S1-{candidate_sha[:12]}.json").write_text(
        json.dumps({"approved": True, "candidate_sha": candidate_sha}), encoding="utf-8"
    )
    (artifact_directory / "delivery-S1.json").write_text(
        json.dumps(
            {
                "state": "github_completed",
                "checks": {"candidate_sha": candidate_sha, "ready": True},
                "pr": {"number": 12, "candidate_sha": candidate_sha},
                "merge": {"merged": True, "sha": merge_sha, "base_sync": {"synced_sha": merge_sha}},
                "issue_closure": {"complete": True},
            }
        ),
        encoding="utf-8",
    )
    receipt_directory = tmp_path / "github-receipts"
    receipt_directory.mkdir()
    (receipt_directory / ".spec-runner-github-receipts.json").write_text(
        json.dumps(
            {
                "tickets:runner-complete:S1": {
                    "repository": "owner/repo",
                    "complete": True,
                    "relation_evidence": {"native": True},
                    "operation_id": "tickets:runner-complete:S1",
                    "issues": [{"number": 20}, {"number": 21}],
                }
            }
        ),
        encoding="utf-8",
    )
    status = {
        "run": {"run_id": run_id, "state": "completed"},
        "workers": [
            {"worker_id": f"codex_sdk:{run_id}:codex_{role}:S1", "external_thread_id": f"thread-{role}", "external_turn_id": f"turn-{role}", "backend_kind": "codex_sdk", "model": "gpt-test", "approval_mode": "deny_all", "state": "completed"}
            for role in ("planning", "implementation", "review")
        ],
    }
    _write_worker_receipts(artifact_directory, status["workers"])
    candidate = {
        "schema_version": "spec-runner-candidate-receipt/v1",
        "outcome": "verified",
        "candidate_sha": candidate_sha,
        "acceptance_version": "ticket-1",
        "checks": [{"command": ["python", "-c", "pass"], "acceptance": ["A1"], "passed": True}],
        "write_scope": {"allowed_paths": ["codex-worker"], "changed_paths": ["codex-worker/src/example.py"]},
    }
    (artifact_directory / "candidate-S1.json").write_text(json.dumps(candidate), encoding="utf-8")
    (artifact_directory / f"review-S1-{candidate_sha[:12]}.json").write_text(
        json.dumps({"approved": True, "candidate_sha": candidate_sha, "review_digest": "review-1"}), encoding="utf-8"
    )
    (artifact_directory / "delivery-S1.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "spec_key": "S1",
                "state": "github_completed",
                "checks": {"candidate_sha": candidate_sha, "ready": True},
                "pr": {"number": 12, "url": "https://example.test/pr/12", "candidate_sha": candidate_sha},
                "merge": {"merged": True, "sha": merge_sha, "base_sync": {"synced_sha": merge_sha}},
                "cleanup": {"outcome": "cleaned"},
                "issue_closure": {"complete": True, "issues": [{"number": 20, "state": "closed"}]},
            }
        ),
        encoding="utf-8",
    )

    def issue_reader(repository: str, number: int) -> dict:
        assert repository == "owner/repo"
        return {"number": number, "title": f"Issue {number}", "body": f"spec-runner-run:{run_id}", "url": f"https://example.test/{number}", "labels": [{"name": "tc083"}]}

    errors, evidence, resources, specs = validate_runner_evidence(
        status,
        artifact_directory=artifact_directory,
        runner_run_id=run_id,
        repository="owner/repo",
        marker=marker,
        issue_reader=issue_reader,
    )

    assert errors == ()
    assert specs == ("S1",)
    assert len(evidence) == 1
    assert len(resources) == 3


def test_runner_evidence_requires_cleanup_receipt_and_direct_origin_readback(tmp_path) -> None:
    run_id = "runner-incomplete-delivery"
    artifact_directory = tmp_path / "artifacts" / run_id
    artifact_directory.mkdir(parents=True)
    (artifact_directory / "spec-plan.json").write_text(
        json.dumps({"outcome": "planned", "specs": [{"key": "S1"}]}), encoding="utf-8"
    )
    (artifact_directory / "ticket-plan-S1.json").write_text(
        json.dumps({"outcome": "planned", "spec_key": "S1"}), encoding="utf-8"
    )
    candidate_sha = "a" * 40
    merge_sha = "b" * 40
    candidate = {
        "schema_version": "spec-runner-candidate-receipt/v1",
        "outcome": "verified",
        "candidate_sha": candidate_sha,
        "acceptance_version": "ticket-1",
        "checks": [{"command": ["python", "-c", "pass"], "acceptance": ["A1"], "passed": True}],
        "write_scope": {"allowed_paths": ["src"], "changed_paths": ["src/example.py"]},
    }
    (artifact_directory / "candidate-S1.json").write_text(json.dumps(candidate), encoding="utf-8")
    (artifact_directory / f"review-S1-{candidate_sha[:12]}.json").write_text(
        json.dumps({"approved": True, "candidate_sha": candidate_sha, "review_digest": "review-1"}), encoding="utf-8"
    )
    (artifact_directory / "delivery-S1.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "spec_key": "S1",
                "state": "github_completed",
                "checks": {"candidate_sha": candidate_sha, "ready": True},
                "pr": {"number": 12, "url": "https://example.test/pr/12", "candidate_sha": candidate_sha},
                "merge": {
                    "merged": True,
                    "sha": merge_sha,
                    "base_sync": {"outcome": "fast_forwarded"},
                },
                "issue_closure": {"complete": True, "issues": [{"number": 20, "state": "closed"}]},
            }
        ),
        encoding="utf-8",
    )
    status = {
        "run": {"run_id": run_id, "state": "completed"},
        "workers": [
            {
                "worker_id": f"codex_sdk:{run_id}:codex_{role}:S1",
                "external_thread_id": f"thread-{role}",
                "external_turn_id": f"turn-{role}",
                "backend_kind": "codex_sdk",
                "state": "completed",
            }
            for role in ("planning", "implementation", "review")
        ],
    }
    _write_worker_receipts(artifact_directory, status["workers"])

    errors, evidence, _, _ = validate_runner_evidence(
        status,
        artifact_directory=artifact_directory,
        runner_run_id=run_id,
        repository="owner/repo",
        marker="<!-- tc083:manifest-1 -->",
    )

    assert not evidence
    assert any("origin readback S1" in error for error in errors)
    assert any("cleanup receipt S1" in error for error in errors)


def test_default_manifest_path_is_outside_docs_specs() -> None:
    assert "docs" not in str(default_manifest_path()).lower()
    assert "specs" not in str(default_manifest_path()).lower()
