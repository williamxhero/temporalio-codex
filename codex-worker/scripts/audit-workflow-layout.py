import asyncio
import argparse
import json

from temporalio.client import Client


async def main(pause=False):
    client = await Client.connect("127.0.0.1:7233")
    rows = []
    async for item in client.list_workflows('ExecutionStatus="Running"'):
        handle = client.get_workflow_handle(item.id, run_id=item.run_id)
        description = await handle.describe()
        row = {"id": item.id, "run_id": item.run_id, "type": item.workflow_type}
        if pause and item.workflow_type in {"RequirementDeliveryWorkflow", "SpecExecutionWorkflow"}:
            row["paused"] = await handle.execute_update("pause", result_type=bool)
        parent = description.raw_description.workflow_execution_info.parent_execution
        row["parent"] = parent.workflow_id if parent else None
        rows.append(row)
    print(json.dumps(rows, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pause", action="store_true")
    asyncio.run(main(parser.parse_args().pause))
