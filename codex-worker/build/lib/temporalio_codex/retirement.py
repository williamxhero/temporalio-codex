from __future__ import annotations

import argparse
import importlib
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path

from temporalio_codex.entry_models import ENTRY_CONTRACT_VERSION

RETIREMENT_SCHEMA_VERSION = "temporalio-codex-retirement/v1"
_HANDOFF_MARKERS = (
    "spec_runner_handoff.py",
    "new requests enter the runner directly",
    "control root",
    "launch key",
)
_RETIREMENT_MARKERS = (
    "does not run the legacy qualification gate",
    "do not create a legacy control database",
    "legacy and runner executions are isolated",
)


@dataclass(frozen=True)
class RetirementEvidence:
    schema_version: str
    status: str
    contract_version: str
    skill_root: str
    package_version: str
    run_id: str
    evidence_refs: tuple[str, ...] = ()
    legacy_state_paths: tuple[str, ...] = ()
    reason: str = ""


def _package_version() -> str:
    package = importlib.import_module("temporalio_codex")
    return str(getattr(package, "__version__", ""))


def _skill_errors(skill_root: Path) -> list[str]:
    skill_path = skill_root / "SKILL.md"
    handoff_path = skill_root / "scripts" / "spec_runner_handoff.py"
    errors: list[str] = []
    if not skill_path.is_file():
        errors.append("installed implement-needs Skill is missing")
    if not handoff_path.is_file():
        errors.append("public Spec Runner handoff script is missing")
    if errors:
        return errors

    skill_text = skill_path.read_text(encoding="utf-8").lower()
    for marker in _HANDOFF_MARKERS:
        if marker not in skill_text:
            errors.append(f"Skill is missing public handoff marker: {marker}")
    for marker in _RETIREMENT_MARKERS:
        if marker not in skill_text:
            errors.append(f"Skill is missing retirement marker: {marker}")

    handoff_text = handoff_path.read_text(encoding="utf-8")
    if "spec_runner.cli" not in handoff_text and "spec-runner" not in handoff_text:
        errors.append("handoff does not invoke the public Spec Runner CLI")
    if re.search(r"\b(dispatch|supervisor|managed_recovery|advance_runtime)\b", handoff_text):
        errors.append("handoff invokes a legacy controller path")
    return errors


def _legacy_state_paths(control_root: Path) -> tuple[str, ...]:
    if not control_root.exists():
        return ()
    names = {
        "legacy",
        "legacy.sqlite",
        "control.sqlite",
        "leases",
        "recovery",
        "dispatch",
        "supervisor",
        "takeover",
    }
    return tuple(
        sorted(
            path.relative_to(control_root).as_posix()
            for path in control_root.rglob("*")
            if path.name.lower() in names
        )
    )


def qualify_public_entry(
    *,
    skill_root: str | Path,
    control_root: str | Path,
    run_id: str,
    contract_version: str = ENTRY_CONTRACT_VERSION,
) -> RetirementEvidence:
    """Qualify the installed public handoff and fresh-run legacy boundary."""
    root = Path(skill_root).expanduser().resolve()
    control = Path(control_root).expanduser().resolve()
    errors = _skill_errors(root)
    if contract_version != ENTRY_CONTRACT_VERSION:
        errors.append(f"unsupported entry contract: {contract_version}")
    legacy_paths = _legacy_state_paths(control)
    if legacy_paths:
        errors.append("fresh Runner state contains legacy controller paths")
    package_version = _package_version()
    if not run_id.strip():
        errors.append("retirement qualification requires run_id")
    status = "verified" if not errors else "not_verified"
    return RetirementEvidence(
        schema_version=RETIREMENT_SCHEMA_VERSION,
        status=status,
        contract_version=contract_version,
        skill_root=str(root),
        package_version=package_version,
        run_id=run_id,
        evidence_refs=(
            f"skill:{root / 'SKILL.md'}",
            f"handoff:{root / 'scripts' / 'spec_runner_handoff.py'}",
            f"control-root:{control}",
        ),
        legacy_state_paths=legacy_paths,
        reason="; ".join(errors),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Qualify the public implement-needs retirement boundary")
    parser.add_argument("--skill-root", type=Path, required=True)
    parser.add_argument("--control-root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)
    result = qualify_public_entry(
        skill_root=args.skill_root,
        control_root=args.control_root,
        run_id=args.run_id,
    )
    print(json.dumps(asdict(result), ensure_ascii=False, sort_keys=True))
    return 0 if result.status == "verified" else 2
