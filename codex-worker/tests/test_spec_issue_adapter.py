from unittest.mock import AsyncMock

from temporalio_codex.spec_issue_adapter import (
    FakeSpecIssueGateway,
    GhCliSpecIssueGateway,
    SpecDraft,
    SpecPublicationInput,
    SpecPublicationStatus,
    publish_specs,
    topological_specs,
    validate_spec_graph,
)


def draft(key: str, dependencies=()):
    return SpecDraft(
        key=key,
        title=f"{key} delivery",
        scope=f"Deliver {key}",
        acceptance_criteria=(f"{key} is verifiable",),
        testing_decisions=(f"Test {key}",),
        dependencies=tuple(dependencies),
        provenance=("decision:1",),
    )


def publication_input(drafts):
    return SpecPublicationInput(
        repository="owner/repo",
        umbrella_issue_number=1,
        source_identity="source-1",
        operation_id="publish-1",
        drafts=tuple(drafts),
    )


def test_dependency_graph_rejects_missing_reference_and_cycle() -> None:
    assert validate_spec_graph((draft("a", ("missing",)),)) == (
        "SPEC a has unresolved dependencies: missing",
    )
    cycle = validate_spec_graph((draft("a", ("b",)), draft("b", ("a",))))
    assert "cycle" in cycle[0]


def test_topological_order_is_stable() -> None:
    assert tuple(item.key for item in topological_specs((draft("b", ("a",)), draft("a")))) == (
        "a",
        "b",
    )


async def test_publication_creates_parented_specs_in_dependency_order() -> None:
    gateway = FakeSpecIssueGateway()
    result = await publish_specs(
        publication_input((draft("b", ("a",)), draft("a"))), gateway
    )

    assert result.status is SpecPublicationStatus.VERIFIED
    assert [issue.title for issue in result.issues] == [
        "[SPEC a] a delivery",
        "[SPEC b] b delivery",
    ]
    assert all(issue.parent_issue_number == 1 for issue in result.issues)
    assert gateway.parent_links


async def test_publication_is_idempotent_by_operation_identity() -> None:
    gateway = FakeSpecIssueGateway()
    input = publication_input((draft("a"),))

    first = await publish_specs(input, gateway)
    second = await publish_specs(input, gateway)

    assert first == second
    assert len(gateway.issues) == 1


async def test_publication_unknown_preserves_partial_evidence() -> None:
    gateway = FakeSpecIssueGateway(fail_create=True)
    result = await publish_specs(publication_input((draft("a"),)), gateway)

    assert result.status is SpecPublicationStatus.UNKNOWN
    assert result.issues == ()
    assert "readback" in result.reason


async def test_spec_readback_scans_all_pages_and_matches_exact_operation() -> None:
    gateway = GhCliSpecIssueGateway("owner/repo")
    gateway._api = AsyncMock(return_value=[
        [{"number": 1, "id": 1, "title": "wrong", "body": "Operation identity: publish:a-extra"}],
        [{"number": 2, "id": 2, "title": "right", "body": "Operation identity: publish:a\n"}],
    ])

    result = await gateway.find_by_operation("publish:a")

    assert result is not None
    assert result.number == 2
    gateway._api.assert_awaited_once_with("issues?state=all&per_page=100", paginate=True)
