import pytest

from acceptance.test_whole_flow import whole_flow_input
from temporalio_codex.entry import stable_run_id
from temporalio_codex.entry_models import (
    ENTRY_CONTRACT_VERSION,
    RequirementDeliveryRequest,
    RequirementSource,
)
from temporalio_codex.planning_models import SourceOrigin


def request(*, text: str = "deliver a governed change", launch_key: str = "case-1"):
    original = whole_flow_input()
    plan = original.__class__(
        planning=original.planning.__class__(
            origin=SourceOrigin.TEXT,
            source_text=text,
            umbrella_issue_number=original.planning.umbrella_issue_number,
            specs=original.planning.specs,
            grill_answers=original.planning.grill_answers,
            confirmation_operation_id=original.planning.confirmation_operation_id,
            publication_operation_id=original.planning.publication_operation_id,
        ),
        scheduler=original.scheduler,
        codex=original.codex,
        deliveries=original.deliveries,
        summary=original.summary,
    )
    source = RequirementSource(origin=SourceOrigin.TEXT, text=text)
    return RequirementDeliveryRequest(
        source=source,
        repository="williamxhero/temporalio-codex",
        artifact_roots=("codex-worker",),
        execution_plan=plan,
        launch_key=launch_key,
    )


def test_public_request_binds_source_identity_and_contract_version() -> None:
    value = request()

    assert value.contract_version == ENTRY_CONTRACT_VERSION
    assert value.execution_plan.planning.source_text == value.source.text
    assert value.input_identity
    assert stable_run_id(value) == "foundation:request:#43"


def test_same_launch_key_has_stable_run_id_but_input_drift_has_new_identity() -> None:
    first = request()
    retry = request()
    changed = request(text="a different governed change")

    assert stable_run_id(first) == stable_run_id(retry)
    assert first.input_identity == retry.input_identity
    assert stable_run_id(first) == stable_run_id(changed)
    assert first.input_identity != changed.input_identity


def test_historical_chat_requires_reference_and_preserves_reference_boundary() -> None:
    source = RequirementSource(
        origin=SourceOrigin.HISTORICAL_CHAT,
        reference="artifact://chat-123",
    )
    plan = whole_flow_input()
    historical_plan = plan.__class__(
        planning=plan.planning.__class__(
            origin=SourceOrigin.HISTORICAL_CHAT,
            source_reference="artifact://chat-123",
            umbrella_issue_number=plan.planning.umbrella_issue_number,
            specs=plan.planning.specs,
            grill_answers=plan.planning.grill_answers,
            confirmation_operation_id=plan.planning.confirmation_operation_id,
            publication_operation_id=plan.planning.publication_operation_id,
        ),
        scheduler=plan.scheduler,
        codex=plan.codex,
        deliveries=plan.deliveries,
        summary=plan.summary,
    )
    value = RequirementDeliveryRequest(
        source=source,
        repository="williamxhero/temporalio-codex",
        artifact_roots=("codex-worker",),
        execution_plan=historical_plan,
        launch_key="chat-1",
    )

    assert value.source.reference == "artifact://chat-123"
    assert value.source.text is None
    assert historical_plan.planning.source_text is None


@pytest.mark.parametrize(
    "source",
    [
        RequirementSource(origin=SourceOrigin.TEXT, text="valid"),
        RequirementSource(origin=SourceOrigin.HISTORICAL_CHAT, reference="artifact://chat"),
    ],
)
def test_request_rejects_unsafe_artifact_roots(source) -> None:
    with pytest.raises(ValueError, match="artifact roots"):
        RequirementDeliveryRequest(
            source=source,
            repository="williamxhero/temporalio-codex",
            artifact_roots=("../outside",),
            execution_plan=whole_flow_input(),
            launch_key="unsafe",
        )
