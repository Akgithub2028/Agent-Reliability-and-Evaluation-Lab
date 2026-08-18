"""Example plugins for the PluginManager hot-reload test-suite and demos."""
from __future__ import annotations

from typing import Any

from mcp_agent.enhancements.plugin import Plugin


class HelloPlugin(Plugin):
    """A trivial plugin used by tests."""

    name = "HelloPlugin"

    def __init__(self) -> None:
        super().__init__()
        self.greeting = "hello"

    async def setup(self, app: Any) -> None:
        # In a real plugin you'd register workflow patterns, tools, hooks.
        if app is not None:
            installed = getattr(app, "installed_plugins", None)
            if installed is not None:
                installed.append(self.name)

    async def teardown(self) -> None:
        pass


class CounterPlugin(Plugin):
    """Counts the number of times its `setup()` is called — used by hot-reload tests."""

    name = "CounterPlugin"

    # Class-level counter so we can observe hot-reload effects across instances.
    setup_calls: int = 0

    def __init__(self) -> None:
        super().__init__()
        self.label = "v1"

    async def setup(self, app: Any) -> None:
        CounterPlugin.setup_calls += 1
        if app is not None:
            counter = getattr(app, "counter_plugin_setups", None)
            if counter is not None:
                counter.append(self.label)

    async def teardown(self) -> None:
        pass
