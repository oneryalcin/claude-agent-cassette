"""Replay a saved cassette through a real ClaudeSDKClient — no API key, no network.

    python examples/replay_cassette.py

The recorded frames flow through the SDK's real message parser; we just print the
typed messages your app would receive.
"""

import asyncio
from pathlib import Path

from claude_agent_cassette import load_cassette, replay

CASSETTE = Path(__file__).parent / "cassettes" / "hello_world.jsonl"


async def main() -> None:
    async with replay(load_cassette(CASSETTE)) as client:
        async for message in client.receive_messages():
            kind = type(message).__name__
            print(f"{kind}: {getattr(message, 'result', '') or ''}")
            if kind == "ResultMessage":
                break


if __name__ == "__main__":
    asyncio.run(main())
