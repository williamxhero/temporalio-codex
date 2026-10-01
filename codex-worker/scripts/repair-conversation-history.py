import argparse
import asyncio
import json

from temporalio.client import Client
from temporalio.worker import Replayer

from temporalio_codex.conversation_store import ConversationStore
from temporalio_codex.delivery_workflows import DeliveryWorkflow
from temporalio_codex.planning_workflows import RequirementPlanningWorkflow
from temporalio_codex.spec_workflows import SpecExecutionWorkflow
from temporalio_codex.summary_workflows import DeliverySummaryWorkflow
from temporalio_codex.ticket_workflows import TicketSchedulerWorkflow
from temporalio_codex.whole_flow_workflows import RequirementDeliveryWorkflow
from temporalio_codex.workflows import CodexRunWorkflow


async def main(args):
    client = await Client.connect(args.target_host)
    executions = {}
    async for execution in client.list_workflows():
        executions[(execution.id, execution.run_id)] = execution
    replayer = Replayer(workflows=[
        RequirementDeliveryWorkflow, SpecExecutionWorkflow, RequirementPlanningWorkflow,
        TicketSchedulerWorkflow, CodexRunWorkflow, DeliveryWorkflow, DeliverySummaryWorkflow,
    ])
    store = ConversationStore(args.database)
    result = {"imports": [], "linked": 0, "replayed": 0, "replay_errors": []}
    try:
        for source in args.source:
            result["imports"].append({"source": source, **store.import_history(source)})
        descriptions = {}
        for key, execution in executions.items():
            handle = client.get_workflow_handle(*key[:1], run_id=key[1])
            descriptions[key] = await handle.describe()
            if args.replay and execution.workflow_type in {
                "RequirementDeliveryWorkflow", "SpecExecutionWorkflow", "TicketSchedulerWorkflow", "CodexRunWorkflow",
            }:
                try:
                    await replayer.replay_workflow(await handle.fetch_history())
                    result["replayed"] += 1
                except Exception as error:
                    result["replay_errors"].append({"workflow": key, "error": str(error)})
        for key in executions:
            ancestor = descriptions[key].raw_description.workflow_execution_info.parent_execution
            if not ancestor.workflow_id:
                ancestor = None
            visited = {key}
            while ancestor is not None:
                scope = (ancestor.workflow_id, ancestor.run_id)
                if scope in visited:
                    raise ValueError("cycle in recorded workflow ancestry")
                visited.add(scope)
                store.link_execution(*scope, *key, namespace=client.namespace)
                result["linked"] += 1
                ancestor = descriptions[scope].raw_description.workflow_execution_info.parent_execution if scope in descriptions else None
                if ancestor is not None and not ancestor.workflow_id:
                    ancestor = None
        result["executions"] = len(executions)
        print(json.dumps(result, indent=2))
        if result["replay_errors"]:
            raise SystemExit(1)
    finally:
        store.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True)
    parser.add_argument("--source", action="append", default=[])
    parser.add_argument("--target-host", default="127.0.0.1:7233")
    parser.add_argument("--replay", action="store_true")
    asyncio.run(main(parser.parse_args()))
