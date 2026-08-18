"""Tests for A2A protocol (P1.2)."""
from __future__ import annotations

import asyncio
import pytest

from mcp_agent.enhancements.a2a import (
    A2AClient,
    A2AError,
    A2AServer,
    A2ATask,
    A2ATaskNotFoundError,
    A2ATimeoutError,
    AgentCard,
    HybridMCPA2AGateway,
    TaskState,
)


# ---------------------------------------------------------------------------
# AgentCard
# ---------------------------------------------------------------------------


def test_agent_card_round_trip() -> None:
    card = AgentCard(
        name="researcher",
        description="Looks things up",
        url="https://example.com/a2a",
        capabilities=["tasks", "streaming"],
        skills=[{"id": "search", "name": "Web search"}],
    )
    d = card.to_dict()
    assert d["name"] == "researcher"
    assert d["capabilities"] == ["tasks", "streaming"]
    parsed = AgentCard.from_dict(d)
    assert parsed.name == "researcher"
    assert parsed.skills == card.skills
    assert parsed.protocol_version == "0.3"


# ---------------------------------------------------------------------------
# A2AServer (in-process)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a2a_server_submit_completes() -> None:
    async def echo(task: A2ATask) -> A2ATask:
        task.artifacts.append({
            "parts": [{"text": f"echo: {task.message.get('parts', [{}])[0].get('text', '')}"}]
        })
        task.state = TaskState.COMPLETED
        return task

    server = A2AServer(AgentCard(name="echo"), handler=echo)
    task = await server.submit_task({"parts": [{"text": "hello"}]})
    assert task.state == TaskState.COMPLETED
    assert task.artifacts
    assert "echo: hello" in task.artifacts[0]["parts"][0]["text"]


@pytest.mark.asyncio
async def test_a2a_server_handler_failure_marks_failed() -> None:
    async def boom(task: A2ATask) -> A2ATask:
        raise RuntimeError("boom")

    server = A2AServer(AgentCard(name="boom"), handler=boom)
    task = await server.submit_task({"parts": [{"text": "hi"}]})
    assert task.state == TaskState.FAILED
    assert "boom" in (task.error or "")


@pytest.mark.asyncio
async def test_a2a_server_get_task_and_list() -> None:
    async def ok(task: A2ATask) -> A2ATask:
        task.state = TaskState.COMPLETED
        return task

    server = A2AServer(AgentCard(name="ok"), handler=ok)
    t1 = await server.submit_task({"parts": [{"text": "a"}]})
    t2 = await server.submit_task({"parts": [{"text": "b"}]})
    fetched = await server.get_task(t1.id)
    assert fetched.id == t1.id
    listed = await server.list_tasks()
    assert {t.id for t in listed} == {t1.id, t2.id}


@pytest.mark.asyncio
async def test_a2a_server_cancel_marks_canceled() -> None:
    async def never_finish(task: A2ATask) -> A2ATask:
        await asyncio.sleep(10)
        return task

    server = A2AServer(AgentCard(name="slow"), handler=never_finish)
    # Submit in background, then cancel
    task_future = asyncio.ensure_future(server.submit_task({"parts": [{"text": "hi"}]}))
    await asyncio.sleep(0.05)  # let the server record the task
    # Cancel via direct API
    listed = await server.list_tasks()
    assert listed
    tid = listed[0].id
    canceled = await server.cancel_task(tid)
    assert canceled.state == TaskState.CANCELED
    task_future.cancel()
    try:
        await task_future
    except (asyncio.CancelledError, Exception):
        pass


# ---------------------------------------------------------------------------
# A2AClient (in-process transport)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a2a_client_inproc_send_and_wait() -> None:
    async def echo(task: A2ATask) -> A2ATask:
        task.artifacts.append({"parts": [{"text": "pong"}]})
        task.state = TaskState.COMPLETED
        return task

    server = A2AServer(AgentCard(name="echo"), handler=echo)
    async with A2AClient(transport="inproc", inproc_server=server) as client:
        card = await client.discover()
        assert card.name == "echo"
        task = await client.send_task_and_wait({"parts": [{"text": "ping"}]})
        assert task.state == TaskState.COMPLETED
        assert task.artifacts[0]["parts"][0]["text"] == "pong"


@pytest.mark.asyncio
async def test_a2a_client_get_task_not_found_raises() -> None:
    server = A2AServer(AgentCard(name="x"))
    async with A2AClient(transport="inproc", inproc_server=server) as client:
        with pytest.raises(A2ATaskNotFoundError):
            await client.get_task("nope")


@pytest.mark.asyncio
async def test_a2a_client_send_task_and_wait_times_out() -> None:
    async def stuck(task: A2ATask) -> A2ATask:
        await asyncio.sleep(10)
        return task

    server = A2AServer(AgentCard(name="stuck"), handler=stuck)
    async with A2AClient(transport="inproc", inproc_server=server) as client:
        with pytest.raises(A2ATimeoutError):
            await client.send_task_and_wait({"parts": [{"text": "x"}]}, timeout=0.2)


# ---------------------------------------------------------------------------
# Hybrid gateway
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hybrid_gateway_registers_inproc_and_lists_tools() -> None:
    async def echo(task: A2ATask) -> A2ATask:
        task.artifacts.append({"parts": [{"text": "pong"}]})
        task.state = TaskState.COMPLETED
        return task

    server = A2AServer(AgentCard(name="echo", description="Echoes back"), handler=echo)
    gw = HybridMCPA2AGateway()
    gw.register_inproc(server, name="echo")
    tools = gw.list_tools()
    assert tools
    assert tools[0]["name"] == "a2a__echo"
    assert "Echoes back" in tools[0]["description"]


@pytest.mark.asyncio
async def test_hybrid_gateway_dispatches_tool_call_to_agent() -> None:
    async def echo(task: A2ATask) -> A2ATask:
        text = task.message.get("parts", [{}])[0].get("text", "")
        task.artifacts.append({"parts": [{"text": f"echo:{text}"}]})
        task.state = TaskState.COMPLETED
        return task

    server = A2AServer(AgentCard(name="echo"), handler=echo)
    gw = HybridMCPA2AGateway()
    gw.register_inproc(server, name="echo")
    result = await gw.call_tool("a2a__echo", {"message": {"parts": [{"text": "hi"}]}})
    assert result["state"] == "completed"
    assert result["artifacts"][0]["parts"][0]["text"] == "echo:hi"


@pytest.mark.asyncio
async def test_hybrid_gateway_unknown_agent_raises() -> None:
    gw = HybridMCPA2AGateway()
    with pytest.raises(A2AError):
        await gw.call("nobody", {"parts": []})


@pytest.mark.asyncio
async def test_hybrid_gateway_tool_name_prefix_validation() -> None:
    gw = HybridMCPA2AGateway()
    with pytest.raises(A2AError):
        await gw.call_tool("not_prefixed", {})
