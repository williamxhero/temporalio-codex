# Spec Runner 测试机制调研

## 目的

本记录整理对 `C:\Users\will\.codex\skills\spec-runner`、
`C:\Users\will\.codex\skills\implement-needs` 以及 GitHub 仓库
`williamxhero/skills` 测试资产的静态调研结果。

本文是测试机制调研记录，不是 SPEC、需求副本或交付验收报告。SPEC 和
ticket 仍以 GitHub Issue 为唯一来源。

## 测试分层

### 1. Spec Runner 普通测试

位置：

- `spec-runner/tests/`
- `spec-runner/acceptance/`

主要特点：

- 使用临时 Git 仓库、Fake SDK、Fake worker、Fake GitHub transport 和
  `monkeypatch`/mock。
- 覆盖 Runner 状态机、SQLite 持久化、恢复、候选提交、write scope、review、
  本地 merge、GitHub adapter 合约和 cleanup。
- 可以证明本地代码在合成输入下满足契约，但不能单独证明真实 GitHub Issue、
  PR、check、merge 或远程 cleanup 已发生。

关键示例：

- `acceptance/test_production_planning.py` 用 Fake worker 和 Fake tracker 验证
  Grill、SpecPlan、TicketPlan、依赖顺序和 production queue。
- `acceptance/test_github_production_boundary.py` 用 Fake GitHub 验证等待 CI、
  merge authorization、失败候选恢复和 cleanup 顺序。
- `acceptance/test_github_delivery_contracts.py` 验证 GitHub API 响应解析、分页、
  check 状态、PR readback 和 merge 前置条件。
- `tests/test_codex_adapter.py` 用 Fake SDK 验证 thread/turn identity、resume、
  `deny_all` approval policy 和控制中断边界。

### 2. CI 中运行的测试

`.github/workflows/spec-runner.yml` 的 contract job 在 Windows 和 Ubuntu 上运行：

1. `spec-runner/tests` 的 unittest 套件。
2. `implement-needs/tests/test_spec_runner_handoff.py` 的 handoff 测试。
3. wheel 构建、无源码路径安装和 public CLI deterministic fault matrix。

CI 不会自动运行 live Codex SDK，也不会自动创建 GitHub Issue、PR 或执行远程
merge。这些副作用被明确排除在普通 CI 之外。

### 3. Spec Runner live harness

位置：`spec-runner/acceptance/harness/`

这是显式 opt-in 的真实环境入口，与普通测试不同：

- `live_skill_probe.py`：使用已认证 Codex SDK 读取当前 Skill 并执行真实 turn。
- `live_impl_probe.py`：在 run-marked fixture 目录中执行真实实现 turn，并检查
  worker 的写入范围。
- `live_review_probe.py`：在候选 SHA 上执行独立的只读 review turn。
- `live_update_probe.py`：在同一 SDK thread 上验证 Skill 内容更新后的恢复行为。
- `live_native_relations.py`：在 `williamxhero/skills` 创建带 marker 的真实
  GitHub Issue，写入 native parent/blocked-by 关系，再对精确匹配的 Issue 做
  readback 和 cleanup。

`acceptance/README.md` 规定所有真实资源必须带 run marker、写入 manifest，并在
cleanup 前完成 readback。worker 只能修改
`spec-runner/acceptance/fixture-app/<run-marker>/`。

### 4. Implement Needs 旧 qualification 资产

位置：

- `C:\Users\will\.codex\skills\implement-needs\tests\`
- `C:\Users\will\.codex\skills\implement-needs\validation\`

这些测试主要验证旧 Controller、SQLite、任务后端、恢复、证据、生命周期和
qualification validator。大量测试使用 fake backend、合成 task tree 和合成
evidence。

`validation/scenarios/whole-spec-v1.json` 定义了完整 whole-spec 拓扑：

- 1 个 umbrella SPEC。
- 3 个按依赖顺序排列的 child SPEC。
- 8 个 tickets。
- 1 个 Grill task、1 个 planning task、3 个 SPEC tasks。
- 3 个 SPEC PR。
- 禁止 ticket-level implementation artifacts。

`references/spec-delivery.md` 要求每个 SPEC 使用一个独立实现任务，依次完成
ticket、测试、独立 review、merge、Issue 关闭、任务归档和 readback，然后才能
进入下一个 SPEC。子任务的最终消息只是声明，控制器仍需独立验证 merge、测试、
GitHub 状态和归档状态。

## 已有真实 acceptance run

之前已启动过一次真实 GitHub acceptance run，仓库为
`williamxhero/skills`：

- marker：`SRAC-20260928-63c78bca0d0f`
- run id：`e16e6ac1-c779-472f-a0b8-6dccf72ec42e`
- 结果：`failed`
- 失败阶段：`codex_implementation`

真实完成并有 readback 的部分：

- Grill。
- SpecPlan，生成 `S1 -> S2 -> S3` 三个依赖 SPEC。
- S1 TicketPlan。
- GitHub Issue `#350`（S1）和 `#351`（S1 ticket）。
- `#351` 到 `#350` 的 native Parent 关系。
- 真实 Codex SDK thread/turn，以及 S1 fixture 目录中的实现代码和测试文件。

失败原因是 Runner 的 artifact 路径解释不兼容：

- SDK 的 `repository_path` 被设置为 write scope 根目录。
- Runner 的实现 prompt 要求 artifact 使用 write-scope-relative 路径。
- 真实 `implement-spec` 返回了 workspace-relative 路径：
  `spec-runner/acceptance/fixture-app/<marker>/task_converter.py`。
- Runner 随后又把该路径拼接到 write scope 根目录，校验时找不到文件，返回
  `implementation_artifact_invalid`。

因此，该 run 尚未证明后续 PR、checks、merge、Issue closure、cleanup 和 push
链路。已生成的实现文件存在，不能替代 Runner 对 artifact、candidate、review、
merge 和 cleanup 的正式 receipt。

持久化证据位于：

```text
C:\Users\will\.codex\skills\spec-runner\acceptance\.runtime\SRAC-20260928-63c78bca0d0f\
```

其中的失败 receipt 应保留，不能改写成成功结果。

## 结论

1. 普通 `spec-runner` 测试是本地契约测试，不是真实 GitHub 全链路测试。
2. `implement-needs` qualification 测试验证流程和证据规则，不会自动证明真实
   GitHub 资源已经创建、合并或清理。
3. 只有 live harness 或真实 production Runner run 才能验证真实 SDK、GitHub
   Issue、PR、check、merge 和 cleanup。
4. 当前已有真实 run 证明了真实 Issue publication、native Parent readback 和
   真实实现 worker 可以运行，但在 artifact 边界失败，因此整体 acceptance
   状态仍是未完成。
5. 后续若修复 Runner，应创建新的 marker 和新的 run；不能复用旧 run 或篡改旧
   receipt。新 run 应重新验证 Issue publication、Parent 关系、每个 SPEC 的
   TicketPlan、实现、候选 check、独立 review、PR、merge、Issue closure、cleanup
   和最终同步。

## 本次调研边界

本记录用于说明测试代码和测试流程的职责边界，不作为新的测试结果，也不表示
完整 acceptance 已通过。未完成的真实 run、失败 receipt、临时 Issue、branch、
worktree 和 SDK thread 仍应按照原 acceptance manifest 独立清理或保留审计证据。
