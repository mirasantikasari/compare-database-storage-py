import asyncio
import json
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

# Dedicated pool for long-running scans (S3 listing, MySQL reads, Excel writes) so a slow or
# stuck scan can never starve the asyncio event loop that serves every other request/heartbeat.
_executor = ThreadPoolExecutor(max_workers=16, thread_name_prefix="scan-worker")

_DONE = object()
HEARTBEAT_SECONDS = 15


def sse_format(event: str, data: dict[str, Any]) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()


async def sse_stream(work_fn: Callable[[Callable[[str, dict], None]], None]):
    """
    Runs `work_fn(emit)` on a background thread and turns each `emit(event, data)` call into an
    SSE frame, with a steady heartbeat comment whenever the worker goes quiet for a while (a
    single bucket/table can take a long time to page through with no progress event in between).
    Errors raised inside `work_fn` are caught and turned into an `error` event instead of
    crashing the stream.
    """
    loop = asyncio.get_event_loop()
    queue: asyncio.Queue = asyncio.Queue()

    def emit(event: str, data: dict) -> None:
        loop.call_soon_threadsafe(queue.put_nowait, (event, data))

    def runner() -> None:
        try:
            work_fn(emit)
        except Exception as error:  # noqa: BLE001 - surfaced to the client as an SSE error event
            emit("error", {"message": str(error)})
        finally:
            loop.call_soon_threadsafe(queue.put_nowait, _DONE)

    loop.run_in_executor(_executor, runner)

    while True:
        try:
            item = await asyncio.wait_for(queue.get(), timeout=HEARTBEAT_SECONDS)
        except asyncio.TimeoutError:
            yield b": heartbeat\n\n"
            continue

        if item is _DONE:
            break

        event, data = item
        yield sse_format(event, data)
