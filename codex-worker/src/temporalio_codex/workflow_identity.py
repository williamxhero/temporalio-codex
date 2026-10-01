import hashlib
import re


def identity_label(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-.")[:80] or "unnamed"


def project_issue_id(repository: str, issue_number: int) -> str:
    project = identity_label(repository.rstrip("/\\").replace("\\", "/").rsplit("/", 1)[-1])
    if issue_number < 1:
        raise ValueError("issue number must be positive")
    return f"{project}:#{issue_number}"


def project_request_id(repository: str, issue_number: int) -> str:
    return project_issue_id(repository, issue_number).replace(":#", ":request:#", 1)


def project_spec_id(repository: str, spec_key: str, launch_identity: str) -> str:
    project = identity_label(repository.rstrip("/\\").replace("\\", "/").rsplit("/", 1)[-1])
    digest = hashlib.sha256(launch_identity.encode("utf-8")).hexdigest()[:20]
    return f"project:{project}:{digest}:spec:{identity_label(spec_key)}"
