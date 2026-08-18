"""Tests for PluginManager + hot-reload (P3.1)."""
from __future__ import annotations

import asyncio
import os
import textwrap
from pathlib import Path

import pytest

from mcp_agent.enhancements.plugin import Plugin, PluginManager, load_plugin


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_plugin_from_dotted_path() -> None:
    pm = PluginManager()
    p = await pm.load_plugin("mcp_agent.enhancements.examples.plugins:HelloPlugin")
    assert p.name == "HelloPlugin"
    assert p.greeting == "hello"
    assert "HelloPlugin" in pm.list_plugins()


@pytest.mark.asyncio
async def test_load_plugin_from_file_path(tmp_path: Path) -> None:
    plugin_src = tmp_path / "myplugin.py"
    plugin_src.write_text(textwrap.dedent("""
        from mcp_agent.enhancements.plugin import Plugin

        class MyPlugin(Plugin):
            name = "MyPlugin"

            async def setup(self, app):
                self._installed = True

            async def teardown(self):
                self._installed = False
    """))
    pm = PluginManager()
    p = await pm.load_plugin(str(plugin_src), name="MyPlugin")
    assert p.name == "MyPlugin"
    assert p._installed is True
    await pm.unload_plugin("MyPlugin")
    assert p._installed is False


@pytest.mark.asyncio
async def test_load_plugin_with_duplicate_name_raises(tmp_path: Path) -> None:
    plugin_src = tmp_path / "myplugin.py"
    plugin_src.write_text(textwrap.dedent("""
        from mcp_agent.enhancements.plugin import Plugin

        class MyPlugin(Plugin):
            name = "MyPlugin"
            async def setup(self, app):
                pass
            async def teardown(self):
                pass
    """))
    pm = PluginManager()
    await pm.load_plugin(str(plugin_src), name="MyPlugin")
    with pytest.raises(ValueError):
        await pm.load_plugin(str(plugin_src), name="MyPlugin")


@pytest.mark.asyncio
async def test_load_plugin_calls_setup_with_app() -> None:
    class FakeApp:
        def __init__(self) -> None:
            self.installed_plugins: list[str] = []

    app = FakeApp()
    pm = PluginManager(app=app)
    await pm.load_plugin("mcp_agent.enhancements.examples.plugins:HelloPlugin")
    assert "HelloPlugin" in app.installed_plugins


# ---------------------------------------------------------------------------
# Hot-reload
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hot_reload_swaps_plugin_instance(tmp_path: Path) -> None:
    plugin_src = tmp_path / "hotplug.py"
    plugin_src.write_text(textwrap.dedent("""
        from mcp_agent.enhancements.plugin import Plugin

        class HotPlug(Plugin):
            name = "HotPlug"
            version = "v1"
            def __init__(self):
                super().__init__()
                self.version = "v1"
            async def setup(self, app):
                app and getattr(app, "versions", None) and app.versions.append(self.version)
            async def teardown(self):
                pass
    """))
    pm = PluginManager(app=type("A", (), {"versions": []})())
    await pm.load_plugin(str(plugin_src), name="HotPlug")
    assert pm.get_plugin("HotPlug").version == "v1"

    # Rewrite the source with v2
    plugin_src.write_text(textwrap.dedent("""
        from mcp_agent.enhancements.plugin import Plugin

        class HotPlug(Plugin):
            name = "HotPlug"
            version = "v2"
            def __init__(self):
                super().__init__()
                self.version = "v2"
            async def setup(self, app):
                app and getattr(app, "versions", None) and app.versions.append(self.version)
            async def teardown(self):
                pass
    """))
    new_p = await pm.hot_reload("HotPlug")
    assert new_p.version == "v2"
    rec = next(r for r in pm.stats()["plugins"] if r["name"] == "HotPlug")
    assert rec["reload_count"] == 1


@pytest.mark.asyncio
async def test_hot_reload_unknown_plugin_raises() -> None:
    pm = PluginManager()
    with pytest.raises(KeyError):
        await pm.hot_reload("does-not-exist")


@pytest.mark.asyncio
async def test_load_plugin_missing_file_raises(tmp_path: Path) -> None:
    pm = PluginManager()
    with pytest.raises(FileNotFoundError):
        await pm.load_plugin(str(tmp_path / "nope.py"))


# ---------------------------------------------------------------------------
# Filesystem watcher (polling fallback)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_watcher_polling_triggers_hot_reload(tmp_path: Path) -> None:
    plugin_src = tmp_path / "watched.py"
    plugin_src.write_text(textwrap.dedent("""
        from mcp_agent.enhancements.plugin import Plugin

        class Watched(Plugin):
            name = "Watched"
            def __init__(self):
                super().__init__()
                self.gen = 1
            async def setup(self, app):
                pass
            async def teardown(self):
                pass
    """))
    pm = PluginManager(hot_reload_enabled=True, poll_interval_s=0.1)
    # Force the polling fallback path even if watchdog is installed.
    pm._use_watchdog = False
    await pm.load_plugin(str(plugin_src), name="Watched")
    await pm.start_watcher()
    try:
        # Rewrite file with gen=2
        plugin_src.write_text(textwrap.dedent("""
            from mcp_agent.enhancements.plugin import Plugin

            class Watched(Plugin):
                name = "Watched"
                def __init__(self):
                    super().__init__()
                    self.gen = 2
                async def setup(self, app):
                    pass
                async def teardown(self):
                    pass
        """))
        # Wait for polling to detect change & reload
        for _ in range(30):
            await asyncio.sleep(0.1)
            if pm.get_plugin("Watched").gen == 2:
                break
        assert pm.get_plugin("Watched").gen == 2
    finally:
        await pm.stop_watcher()
