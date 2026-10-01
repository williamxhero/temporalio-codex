from temporalio import workflow
from temporalio_codex.whole_flow_workflows import RequirementDeliveryWorkflow

with workflow.unsafe.imports_passed_through():
    from temporalio_codex.whole_flow_models import WholeFlowInput, WholeFlowResult


@workflow.defn
class SpecExecutionWorkflow(RequirementDeliveryWorkflow):
    @workflow.run
    async def run(self, input: WholeFlowInput) -> WholeFlowResult:
        if input.execution_layout != "spec" or len(input.scheduler.specs) != 1:
            return self._blocked("SPEC execution requires the single SPEC layout")
        return await super().run(input)
