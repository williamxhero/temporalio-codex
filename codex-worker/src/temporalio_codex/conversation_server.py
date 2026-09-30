from __future__ import annotations

import asyncio
import json
import sqlite3
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
        self._clients: set[asyncio.StreamWriter] = set()
        self._handlers: set[asyncio.Task] = set()

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle,
            self.host,
            self.port,
        )
        self.port = self._server.sockets[0].getsockname()[1]

    async def close(self) -> None:
        if self._server is None:
            return
        self._server.close()
        for writer in tuple(self._clients):
            writer.close()
        for task in tuple(self._handlers):
            task.cancel()
        await asyncio.gather(*self._handlers, return_exceptions=True)
        await self._server.wait_closed()
        self._server = None

    async def _handle(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self._clients.add(writer)
        task = asyncio.current_task()
        if task is not None:
            self._handlers.add(task)
        try:
            request_line = await asyncio.wait_for(reader.readline(), 5)
            if not request_line:
                return
            for _ in range(100):
                header = await asyncio.wait_for(reader.readline(), 5)
                if not header:
                    raise ValueError("incomplete headers")
                if header == b"\r\n":
                    break
            else:
                raise ValueError("too many headers")
            method, target, version = request_line.decode("ascii").strip().split(" ")
            if version not in {"HTTP/1.0", "HTTP/1.1"}:
                raise ValueError("unsupported version")
            if method == "OPTIONS":
                await self._write_response(writer, 204, b"", "text/plain")
                return
            if method != "GET":
                await self._write_response(
                    writer, 405, b"method not allowed", "text/plain"
                )
                return

            parsed = urlsplit(target)
            params = parse_qs(parsed.query, keep_blank_values=True, max_num_fields=16)
            workflow_id = params.get("workflow_id", [""])[0].strip()
            workflow_run_id = params.get("run_id", [""])[0].strip() or None
            namespace = params.get("namespace", [""])[0].strip()
            if parsed.path in {
                "/api/v1/codex/conversations",
                "/api/v1/codex/stream",
            } and any(
                len(params.get(key, ())) != 1 or not params[key][0].strip()
                for key in ("namespace", "workflow_id", "run_id")
            ):
                raise ValueError("namespace, workflow_id and run_id required")
            if parsed.path in {
                "/health",
                "/api/v1/codex/conversations",
                "/api/v1/codex/stream",
            }:
                self.store.check_health()
            if parsed.path == "/health":
                await self._write_json(writer, {"status": "ok"})
            elif parsed.path == "/api/v1/codex/conversations":
                await self._write_json(
                    writer,
                    self.store.snapshot(
                        workflow_id, workflow_run_id, namespace=namespace
                    ),
                )
            elif parsed.path == "/api/v1/codex/stream":
                await self._stream(writer, workflow_id, workflow_run_id, namespace)
            else:
                await self._write_response(writer, 404, b"not found", "text/plain")
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except (TimeoutError, ValueError, UnicodeError):
            await self._write_response(writer, 400, b"malformed request", "text/plain")
        except (sqlite3.Error, OSError):
            await self._write_response(
                writer, 503, b'{"status":"unavailable"}', "application/json"
            )
        finally:
            self._clients.discard(writer)
            if task is not None:
                self._handlers.discard(task)
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()

    async def _stream(
        self,
        writer: asyncio.StreamWriter,
        workflow_id: str,
        workflow_run_id: str | None,
        namespace: str,
    ) -> None:
        snapshot = self.store.snapshot(
            workflow_id, workflow_run_id, namespace=namespace
        )
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
            if snapshot["cursor"] != cursor:
                cursor = snapshot["cursor"]
                writer.write(
                    f"event: conversations\ndata: {json.dumps(snapshot, ensure_ascii=False)}\n\n".encode()
                )
                await writer.drain()
            await asyncio.sleep(0.5)
            try:
                snapshot = self.store.snapshot(
                    workflow_id, workflow_run_id, namespace=namespace
                )
            except (sqlite3.Error, OSError):
                writer.write(b'event: unavailable\ndata: {"status":"unavailable"}\n\n')
                await writer.drain()
                return

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
        reason = {
            200: "OK",
            204: "No Content",
            400: "Bad Request",
            404: "Not Found",
            405: "Method Not Allowed",
            503: "Service Unavailable",
        }[status]
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
