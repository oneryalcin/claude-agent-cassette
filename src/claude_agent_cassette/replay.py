"""High-level replay helper: drive a real ClaudeSDKClient over a cassette."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient

from .tape import RawMessage
from .transport import ReplayTransport


@asynccontextmanager
async def replay(
    messages: list[RawMessage],
    options: Optional[ClaudeAgentOptions] = None,
) -> AsyncIterator[ClaudeSDKClient]:
    """Drive a real ``ClaudeSDKClient`` over a ``ReplayTransport`` fed ``messages``.

    No API key, no subprocess — the recorded frames flow through the SDK's real
    parser. Iterate ``receive_messages()`` and assert on the typed messages your
    app would see.

    Usage::

        from claude_agent_cassette import replay, load_cassette

        async with replay(load_cassette("session.jsonl")) as client:
            kinds = [type(m).__name__ async for m in client.receive_messages()]
            assert "ResultMessage" in kinds
    """
    client = ClaudeSDKClient(
        options=options or ClaudeAgentOptions(),
        transport=ReplayTransport(messages),
    )
    await client.connect()
    try:
        yield client
    finally:
        await client.disconnect()
