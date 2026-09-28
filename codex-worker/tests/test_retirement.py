from __future__ import annotations

from temporalio_codex.retirement import qualify_public_entry


def _skill(root, *, skill_text: str | None = None, handoff_text: str | None = None):
    (root / "scripts").mkdir(parents=True)
    (root / "SKILL.md").write_text(
        skill_text
        or """
        New requests enter the Runner directly.
        The entry does not run the legacy qualification gate.
        Do not create a legacy control database.
        Legacy and Runner executions are isolated.
        spec_runner_handoff.py uses the control root and launch key.
        """,
        encoding="utf-8",
    )
    (root / "scripts" / "spec_runner_handoff.py").write_text(
        handoff_text
        or "from spec_runner.cli import main\n# launch with control root and launch key\n",
        encoding="utf-8",
    )


def test_public_entry_qualification_accepts_runner_only_skill(tmp_path) -> None:
    skill = tmp_path / "skill"
    control = tmp_path / "control"
    _skill(skill)

    result = qualify_public_entry(
        skill_root=skill,
        control_root=control,
        run_id="runner-1",
    )

    assert result.status == "verified"
    assert result.contract_version == "requirement-delivery/v1"
    assert result.legacy_state_paths == ()
    assert result.evidence_refs


def test_public_entry_qualification_rejects_legacy_state_and_handoff(tmp_path) -> None:
    skill = tmp_path / "skill"
    control = tmp_path / "control"
    _skill(skill, handoff_text="import dispatch\n")
    (control / "legacy.sqlite").parent.mkdir(parents=True)
    (control / "legacy.sqlite").write_text("historical", encoding="utf-8")

    result = qualify_public_entry(
        skill_root=skill,
        control_root=control,
        run_id="runner-2",
    )

    assert result.status == "not_verified"
    assert result.legacy_state_paths == ("legacy.sqlite",)
    assert "legacy controller path" in result.reason
    assert "legacy controller paths" in result.reason


def test_public_entry_qualification_rejects_incomplete_skill(tmp_path) -> None:
    skill = tmp_path / "skill"
    control = tmp_path / "control"
    _skill(skill, skill_text="new requests enter the runner directly")

    result = qualify_public_entry(
        skill_root=skill,
        control_root=control,
        run_id="runner-3",
    )

    assert result.status == "not_verified"
    assert "retirement marker" in result.reason
