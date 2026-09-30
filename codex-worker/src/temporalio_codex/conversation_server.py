from __future__ import annotations

import asyncio
import json
from contextlib import suppress
from urllib.parse import parse_qs, urlsplit

from temporalio_codex.conversation_store import ConversationStore


class ConversationServer:
    def __init__(
        self,
        store: ConversationStore,
        *,
        host: str = "127.0.0.1",
        port: int = 18001,
    ) -> None:
        self.store = store
        self.host = host
        self.port = port
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle,
            self.host,
            self.port,
        )

    async def close(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    async def _handle(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            request_line = await reader.readline()
            if not request_line:
                return
            while await reader.readline() != b"\r\n":
                pass
            method, target, _ = request_line.decode("ascii").split(" ", 2)
            if method == "OPTIONS":
                await self._write_response(writer, 204, b"", "text/plain")
                return
            if method != "GET":
                await self._write_response(writer, 405, b"method not allowed", "text/plain")
                return

            parsed = urlsplit(target)
            params = parse_qs(parsed.query)
            workflow_id = params.get("workflow_id", [""])[0].strip()
            workflow_run_id = params.get("run_id", [""])[0].strip() or None
            if parsed.path == "/health":
                await self._write_json(writer, {"status": "ok"})
            elif parsed.path == "/api/v1/codex/conversations":
                await self._write_json(
                    writer,
                    self.store.snapshot(workflow_id, workflow_run_id),
                )
            elif parsed.path == "/api/v1/codex/stream":
                await self._stream(writer, workflow_id, workflow_run_id)
            else:
                await self._write_response(writer, 404, b"not found", "text/plain")
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()

    async def _stream(
        self,
        writer: asyncio.StreamWriter,
        workflow_id: str,
        workflow_run_id: str | None,
    ) -> None:
        writer.write(
            (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/event-stream\r\n"
                "Cache-Control: no-cache\r\n"
                "Connection: keep-alive\r\n"
                "Access-Control-Allow-Origin: *\r\n\r\n"
            ).encode("ascii")
        )
        await writer.drain()
        cursor = -1
        for _ in range(60):
            snapshot = self.store.snapshot(workflow_id, workflow_run_id)
            if snapshot["cursor"] != cursor:
                cursor = snapshot["cursor"]
                writer.write(
                    f"event: conversations\ndata: {json.dumps(snapshot, ensure_ascii=False)}\n\n".encode(
                        "utf-8"
                    )
                )
                await writer.drain()
            await asyncio.sleep(0.5)

    async def _write_json(
        self,
        writer: asyncio.StreamWriter,
        value: object,
    ) -> None:
        body = json.dumps(value, ensure_ascii=False).encode("utf-8")
        await self._write_response(writer, 200, body, "application/json")

    async def _write_response(
        self,
        writer: asyncio.StreamWriter,
        status: int,
        body: bytes,
        content_type: str,
    ) -> None:
        reason = {200: "OK", 204: "No Content", 404: "Not Found", 405: "Method Not Allowed"}[status]
        writer.write(
            (
                f"HTTP/1.1 {status} {reason}\r\n"
                f"Content-Type: {content_type}\r\n"
                f"Content-Length: {len(body)}\r\n"
                "Access-Control-Allow-Origin: *\r\n"
                "Connection: close\r\n\r\n"
            ).encode("ascii")
            + body
        )
        await writer.drain()
