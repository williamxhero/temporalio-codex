# Submitted SPEC issues

The canonical documents are versioned in this directory. The corresponding
GitHub Issues are the active planning surface.

| SPEC | Issue | Dependency |
| --- | --- | --- |
| TC | [#1](https://github.com/williamxhero/temporalio-codex/issues/1) | — |
| TC-00 | [#2](https://github.com/williamxhero/temporalio-codex/issues/2) | TC |
| TC-01 | [#3](https://github.com/williamxhero/temporalio-codex/issues/3) | TC-00 |
| TC-02 | [#4](https://github.com/williamxhero/temporalio-codex/issues/4) | TC-01 |
| TC-03 | [#5](https://github.com/williamxhero/temporalio-codex/issues/5) | TC-01 |
| TC-04 | [#6](https://github.com/williamxhero/temporalio-codex/issues/6) | TC-01, TC-02 |
| TC-05 | [#7](https://github.com/williamxhero/temporalio-codex/issues/7) | TC-02, TC-03, TC-04 |

The project deliberately keeps application behavior in external Temporal
Workers. Temporal Server extension seams are used only for Server concerns;
Codex and GitHub are not added to Server core.
