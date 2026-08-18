"""
A2A (Agent-to-Agent) protocol — client, server, discovery, and hybrid MCP/A2A gateway.

Implements the A2A v0.3 protocol surface as described in the improvement plan:

  * ``AgentCard`` — well-known discovery document (served at ``/.well-known/agent.json``).
  * ``A2ATask`` — task lifecycle object with submitted/working/completed/failed states.
  * ``A2AClient`` — resolves an agent URL → AgentCard, then submits tasks and polls
    or streams their results.
  * ``A2AServer`` — in-process A2A server that dispatches tasks to a registered
    handler; suitable for unit tests and for composing two mcp-agent apps.
  * ``HybridMCPA2AGateway`` — exposes an A2A agent as if it were an MCP tool, so a
    plain MCP client can delegate tasks to remote A2A agents transparently. This
    is the "MCP/A2A gateway" mentioned in the improvement plan.

Everything is async, transport-agnostic (pluggable HTTP via ``httpx.AsyncClient``,
or in-process for tests), and self-contained — it does not depend on any other
part of the codebase, so existing code paths are untouched.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union

try:
    import httpx
    _HAVE_HTTPX = True
except Exception:  # pragma: no cover
    _HAVE_HTTPX = False


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


class TaskState(str, Enum):
    SUBMITTED = "submitted"
    WORKING = "working"
    INPUT_REQUIRED = "input-required"
    COMPLETED = "completed"
    CANCELED = "canceled"
    FAILED = "failed"


TERMINAL_STATES = frozenset({TaskState.COMPLETED, TaskState.CANCELED, TaskState.FAILED})


@dataclass
class AgentCard:
    """The discovery document served at /.well-known/agent.json."""

    name: str
    description: str = ""
    version: str = "0.1.0"
    url: str = ""
    capabilities: List[str] = field(default_factory=lambda: ["tasks"])
    skills: List[Dict[str, Any]] = field(default_factory=list)
    default_input_modes: List[str] = field(default_factory=lambda: ["text"])
    default_output_modes: List[str] = field(default_factory=lambda: ["text"])
    protocol_version: str = "0.3"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "url": self.url,
            "capabilities": self.capabilities,
            "skills": self.skills,
            "defaultInputModes": self.default_input_modes,
            "defaultOutputModes": self.default_output_modes,
            "protocolVersion": self.protocol_version,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "AgentCard":
        return cls(
            name=d["name"],
            description=d.get("description", ""),
            version=d.get("version", "0.1.0"),
            url=d.get("url", ""),
            capabilities=d.get("capabilities", ["tasks"]),
            skills=d.get("skills", []),
            default_input_modes=d.get("defaultInputModes", ["text"]),
            default_output_modes=d.get("defaultOutputModes", ["text"]),
            protocol_version=d.get("protocolVersion", "0.3"),
        )


@dataclass
class A2ATask:
    """An A2A task object. (subset of the spec — message history is optional)."""

    id: str
    state: TaskState = TaskState.SUBMITTED
    message: Optional[Dict[str, Any]] = None
    artifacts: List[Dict[str, Any]] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "state": self.state.value,
            "message": self.message,
            "artifacts": self.artifacts,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "error": self.error,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "A2ATask":
        return cls(
            id=d["id"],
            state=TaskState(d.get("state", "submitted")),
            message=d.get("message"),
            artifacts=d.get("artifacts", []),
            created_at=d.get("createdAt", time.time()),
            updated_at=d.get("updatedAt", time.time()),
            error=d.get("error"),
            metadata=d.get("metadata", {}),
        )


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class A2AError(RuntimeError):
    """Base class for all A2A errors."""


class A2ATaskNotFoundError(A2AError):
    pass


class A2ATimeoutError(A2AError):
    pass


# ---------------------------------------------------------------------------
# Task handler signature
# ---------------------------------------------------------------------------

TaskHandler = Callable[[A2ATask], Awaitable[Union[A2ATask, Dict[str, Any]]]]


# ---------------------------------------------------------------------------
# In-process A2A server
# ---------------------------------------------------------------------------


class A2AServer:
    """
    A minimal, in-process A2A server.

    Real deployments would put an HTTP layer in front of this; the in-process
    flavour is what we use for tests and for hybrid gateway composition.
    """

    def __init__(
        self,
        card: AgentCard,
        handler: Optional[TaskHandler] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.card = card
        self._handler = handler
        self._tasks: Dict[str, A2ATask] = {}
        self._lock = asyncio.Lock()
        self._log = logger or logging.getLogger(__name__)

    def set_handler(self, handler: TaskHandler) -> None:
        self._handler = handler

    async def submit_task(self, message: Dict[str, Any], task_id: Optional[str] = None) -> A2ATask:
        if self._handler is None:
            raise A2AError("No task handler registered on server")
        task = A2ATask(
            id=task_id or str(uuid.uuid4()),
            state=TaskState.SUBMITTED,
            message=message,
        )
        async with self._lock:
            self._tasks[task.id] = task
        # Transition to working
        task.state = TaskState.WORKING
        task.updated_at = time.time()
        try:
            result = await self._handler(task)
            if isinstance(result, A2ATask):
                task = result
            elif isinstance(result, dict):
                task.artifacts = result.get("artifacts", task.artifacts)
                task.message = result.get("message", task.message)
                task.state = TaskState(result.get("state", TaskState.COMPLETED.value))
            else:
                task.state = TaskState.COMPLETED
        except Exception as exc:
            task.state = TaskState.FAILED
            task.error = f"{type(exc).__name__}: {exc}"
            self._log.exception("A2A task %s failed", task.id)
        task.updated_at = time.time()
        async with self._lock:
            self._tasks[task.id] = task
        return task

    async def get_task(self, task_id: str) -> A2ATask:
        async with self._lock:
            if task_id not in self._tasks:
                raise A2ATaskNotFoundError(task_id)
            return self._tasks[task_id]

    async def cancel_task(self, task_id: str) -> A2ATask:
        async with self._lock:
            if task_id not in self._tasks:
                raise A2ATaskNotFoundError(task_id)
            t = self._tasks[task_id]
            t.state = TaskState.CANCELED
            t.updated_at = time.time()
            return t

    async def list_tasks(self) -> List[A2ATask]:
        async with self._lock:
            return list(self._tasks.values())

    def card_dict(self) -> Dict[str, Any]:
        return self.card.to_dict()


# ---------------------------------------------------------------------------
# A2A client (httpx-backed, with in-process transport fallback for tests)
# ---------------------------------------------------------------------------


class A2AClient:
    """
    A2A client.

    Two transports:
      * ``transport="http"`` (default, requires httpx): resolves AgentCard from
        ``{base_url}/.well-known/agent.json`` and POSTs tasks to
        ``{base_url}/tasks``.
      * ``transport="inproc"``: takes an ``A2AServer`` instance and calls it
        directly — used by tests and by the hybrid gateway.

    All public methods are async and return ``A2ATask`` objects.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        *,
        transport: str = "http",
        inproc_server: Optional[A2AServer] = None,
        timeout: float = 30.0,
        logger: Optional[logging.Logger] = None,
    ):
        if transport not in ("http", "inproc"):
            raise ValueError(f"unknown transport {transport!r}")
        if transport == "http" and not base_url:
            raise ValueError("base_url is required for http transport")
        if transport == "inproc" and inproc_server is None:
            raise ValueError("inproc_server is required for inproc transport")
        self._transport = transport
        self._base_url = (base_url or "").rstrip("/")
        self._inproc = inproc_server
        self._timeout = timeout
        self._log = logger or logging.getLogger(__name__)
        self._cached_card: Optional[AgentCard] = None
        self._client: Optional[Any] = None
        if transport == "http" and _HAVE_HTTPX:
            self._client = httpx.AsyncClient(timeout=timeout)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()

    # -- discovery ---------------------------------------------------------

    async def discover(self, force: bool = False) -> AgentCard:
        if self._cached_card is not None and not force:
            return self._cached_card
        if self._transport == "inproc":
            assert self._inproc is not None
            self._cached_card = self._inproc.card
            return self._cached_card

        assert self._client is not None
        url = f"{self._base_url}/.well-known/agent.json"
        r = await self._client.get(url)
        r.raise_for_status()
        self._cached_card = AgentCard.from_dict(r.json())
        return self._cached_card

    # -- task lifecycle ----------------------------------------------------

    async def send_task(self, message: Dict[str, Any], task_id: Optional[str] = None) -> A2ATask:
        if self._transport == "inproc":
            assert self._inproc is not None
            return await self._inproc.submit_task(message, task_id=task_id)
        assert self._client is not None
        payload = {"message": message}
        if task_id:
            payload["id"] = task_id
        r = await self._client.post(f"{self._base_url}/tasks", json=payload)
        r.raise_for_status()
        return A2ATask.from_dict(r.json())

    async def get_task(self, task_id: str) -> A2ATask:
        if self._transport == "inproc":
            assert self._inproc is not None
            return await self._inproc.get_task(task_id)
        assert self._client is not None
        r = await self._client.get(f"{self._base_url}/tasks/{task_id}")
        if r.status_code == 404:
            raise A2ATaskNotFoundError(task_id)
        r.raise_for_status()
        return A2ATask.from_dict(r.json())

    async def cancel_task(self, task_id: str) -> A2ATask:
        if self._transport == "inproc":
            assert self._inproc is not None
            return await self._inproc.cancel_task(task_id)
        assert self._client is not None
        r = await self._client.post(f"{self._base_url}/tasks/{task_id}/cancel")
        r.raise_for_status()
        return A2ATask.from_dict(r.json())

    async def send_task_and_wait(
        self,
        message: Dict[str, Any],
        *,
        poll_interval: float = 0.1,
        timeout: Optional[float] = None,
    ) -> A2ATask:
        """
        Submit a task and poll until it reaches a terminal state.

        For the in-process transport, this is essentially a no-op wrapper around
        ``send_task`` since the server is synchronous, but the API mirrors the
        HTTP path so callers can swap transports freely.
        """
        deadline = (time.time() + timeout) if timeout is not None else None
        task = await self.send_task(message)
        if task.state in TERMINAL_STATES:
            return task
        while True:
            await asyncio.sleep(poll_interval)
            task = await self.get_task(task.id)
            if task.state in TERMINAL_STATES:
                return task
            if deadline is not None and time.time() > deadline:
                raise A2ATimeoutError(f"task {task.id} did not finish in {timeout}s")


# ---------------------------------------------------------------------------
# Hybrid MCP/A2A gateway
# ---------------------------------------------------------------------------


class HybridMCPA2AGateway:
    """
    Bridges a remote A2A agent into the local MCP tool surface.

    Each registered A2A agent becomes available as a callable MCP-style tool
    named ``a2a__<agent_name>``. Internally, calling the tool submits an A2A
    task and waits for the result.

    This is the "hybrid MCP/A2A gateway for legacy interop" called for in
    the improvement plan.

    Example::

        gateway = HybridMCPA2AGateway()
        await gateway.register_remote("https://agent.example.com")
        # Now `gateway.call("agent_name", {"role":"user","parts":[{"text":"hi"}]})`
        # submits an A2A task and returns its artifacts.
    """

    TOOL_PREFIX = "a2a__"

    def __init__(self, logger: Optional[logging.Logger] = None):
        self._agents: Dict[str, A2AClient] = {}
        self._cards: Dict[str, AgentCard] = {}
        self._log = logger or logging.getLogger(__name__)

    async def register_remote(self, base_url: str, name: Optional[str] = None) -> AgentCard:
        client = A2AClient(base_url=base_url, transport="http")
        card = await client.discover()
        key = name or card.name
        self._agents[key] = client
        self._cards[key] = card
        return card

    def register_inproc(self, server: A2AServer, name: Optional[str] = None) -> AgentCard:
        client = A2AClient(transport="inproc", inproc_server=server)
        # discover synchronously for inproc
        card = server.card
        key = name or card.name
        self._agents[key] = client
        self._cards[key] = card
        return card

    def list_tools(self) -> List[Dict[str, Any]]:
        """Return a list of MCP-style tool descriptors for each registered agent."""
        return [
            {
                "name": f"{self.TOOL_PREFIX}{name}",
                "description": card.description or f"A2A agent: {name}",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "message": {
                            "type": "object",
                            "description": "An A2A message (role + parts).",
                        },
                    },
                    "required": ["message"],
                },
            }
            for name, card in self._cards.items()
        ]

    async def call(self, agent_name: str, message: Dict[str, Any], timeout: Optional[float] = None) -> A2ATask:
        if agent_name not in self._agents:
            raise A2AError(f"unknown A2A agent {agent_name!r}; not registered")
        client = self._agents[agent_name]
        return await client.send_task_and_wait(message, timeout=timeout)

    async def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Dispatch an MCP-style tool call to the underlying A2A agent."""
        if not tool_name.startswith(self.TOOL_PREFIX):
            raise A2AError(f"tool name {tool_name!r} does not start with {self.TOOL_PREFIX!r}")
        agent_name = tool_name[len(self.TOOL_PREFIX):]
        msg = arguments.get("message") or arguments
        task = await self.call(agent_name, msg)
        return {
            "task_id": task.id,
            "state": task.state.value,
            "artifacts": task.artifacts,
            "error": task.error,
        }

    async def close(self) -> None:
        for c in self._agents.values():
            await c.close()


__all__ = [
    "TaskState",
    "TERMINAL_STATES",
    "AgentCard",
    "A2ATask",
    "A2AError",
    "A2ATaskNotFoundError",
    "A2ATimeoutError",
    "TaskHandler",
    "A2AServer",
    "A2AClient",
    "HybridMCPA2AGateway",
]
