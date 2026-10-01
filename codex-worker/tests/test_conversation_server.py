import asyncio
import json

from temporalio_codex.conversation_server import ConversationServer
from temporalio_codex.conversation_store import ConversationEvent, ConversationStore


async def request(server, target, raw=None):
    reader, writer = await asyncio.open_connection(server.host, server.port)
    writer.write(raw or f"GET {target} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode())
    await writer.drain()
    writer.write_eof()
    response = await asyncio.wait_for(reader.read(), 2)
    writer.close()
    await writer.wait_closed()
    return response


async def test_http_scope_validation_health_and_unavailable_store(tmp_path):
    store = ConversationStore(tmp_path / "http.db")
    server = ConversationServer(store, port=0)
    await server.start()
    try:
        assert b"200 OK" in await request(server, "/health")
        for query in (
            "",
            "?workflow_id=child",
            "?namespace=default&workflow_id=child&run_id=a&run_id=b",
        ):
            assert b"400 Bad Request" in await request(
                server, "/api/v1/codex/conversations" + query
            )
        scope = "?namespace=default&workflow_id=child&run_id=run"
        response = await request(server, "/api/v1/codex/conversations" + scope)
        assert json.loads(response.split(b"\r\n\r\n")[1]) == {
            "cursor": 0,
            "conversations": [],
        }
        assert b"400 Bad Request" in await request(server, "", raw=b"malformed\r\n\r\n")
        assert b"400 Bad Request" in await request(
            server, "", raw=b"GET /health HTTP/1.1\r\nHost: localhost\r\n"
        )
        store.close()
        for path in (
            "/health",
            "/api/v1/codex/conversations" + scope,
            "/api/v1/codex/stream" + scope,
        ):
            assert b"503 Service Unavailable" in await request(server, path)
    finally:
        await server.close()


async def test_sse_updates_and_reconnect_with_exact_execution(tmp_path):
    store = ConversationStore(tmp_path / "sse.db")
    server = ConversationServer(store, port=0)
    await server.start()
    writers = []
    try:
        for reconnect in range(2):
            reader, writer = await asyncio.open_connection(server.host, server.port)
            writers.append(writer)
            writer.write(
                b"GET /api/v1/codex/stream?namespace=default&workflow_id=child&run_id=run&view=thread HTTP/1.1\r\n\r\n"
            )
            await writer.drain()
            assert b"200 OK" in await reader.readuntil(b"\r\n\r\n")
            initial = await asyncio.wait_for(reader.readuntil(b"\n\n"), 2)
            if reconnect:
                payload = json.loads(initial.split(b"data: ", 1)[1])
                assert payload["conversations"][0]["turns"][0]["messages"]
                assert b"answer" in initial
            else:
                store.append(
                    ConversationEvent(
                        workflow_id="child",
                        scope_workflow_id="parent",
                        operation_id="op",
                        stage="planning",
                        role="planning",
                        thread_id="thread",
                        turn_id="turn",
                        kind="assistant_final",
                        workflow_run_id="run",
                        scope_workflow_run_id="parent-run",
                        text="answer",
                    )
                )
                assert b"answer" in await asyncio.wait_for(reader.readuntil(b"\n\n"), 2)
    finally:
        await server.close()
        for writer in writers:
            writer.close()
            await writer.wait_closed()
        store.close()
