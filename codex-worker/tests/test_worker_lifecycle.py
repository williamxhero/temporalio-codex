import asyncio
import sqlite3

import pytest

from temporalio_codex import worker


@pytest.mark.parametrize("failure", [RuntimeError("Temporal unavailable"), asyncio.CancelledError()])
async def test_run_worker_closes_conversation_resources_when_connect_does_not_complete(
    tmp_path, monkeypatch, failure
):
    observed = {}
    real_server = worker.ConversationServer
    real_store = worker.ConversationStore

    class TrackingServer(real_server):
        async def start(self):
            await super().start()
            observed["port"] = self.port

    async def fail_connect(_target):
        raise failure

    def tracking_store(*args, **kwargs):
        instance = real_store(*args, **kwargs)
        observed["store"] = instance
        return instance

    monkeypatch.setattr(worker, "ConversationServer", TrackingServer)
    monkeypatch.setattr(worker, "ConversationStore", tracking_store)
    monkeypatch.setattr(worker.Client, "connect", fail_connect)
    db = tmp_path / "worker.sqlite3"

    with pytest.raises(type(failure)):
        await worker.run_worker(
            target_host="127.0.0.1:7233",
            conversation_db=str(db),
            conversation_port=0,
        )

    assert observed["port"] > 0
    probe = await asyncio.start_server(lambda _reader, writer: writer.close(), "127.0.0.1", observed["port"])
    probe.close()
    await probe.wait_closed()
    with pytest.raises(sqlite3.ProgrammingError):
        observed["store"]._connection.execute("SELECT 1")
