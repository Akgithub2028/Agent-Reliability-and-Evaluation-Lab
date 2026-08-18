"""
Plugin manager with hot-reload.

Implements improvement-plan items P3.1 (plugin architecture) and supports
hot-reload via ``watchdog`` filesystem events.

Design:

  * A ``Plugin`` is any object exposing:
      - ``name: str``
      - ``setup(app: Any) -> Awaitable[None]``
      - ``teardown() -> Awaitable[None]`` (optional)
  * Plugins are discovered either:
      - by Python dotted path (``my_pkg.my_module:MyPlugin``), or
      - by file path to a ``.py`` file (loaded via ``importlib.util``).
  * The manager keeps a map of name → loaded plugin instance.
  * ``hot_reload(plugin_name)`` re-imports the underlying module and
    replaces the plugin instance in-place (with proper teardown first).
  * Optional filesystem watcher: when the underlying source file changes,
    the manager automatically hot-reloads. Uses ``watchdog`` if available,
    else falls back to a polling loop.

This module is deliberately decoupled from the rest of the codebase. The
"app" object passed to ``setup`` is opaque — plugins can do whatever they
want with it (register workflow patterns, hooks, tools, etc.).
"""
from __future__ import annotations

import asyncio
import importlib
import importlib.util
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional

try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
    _HAVE_WATCHDOG = True
except Exception:  # pragma: no cover
    _HAVE_WATCHDOG = False
    FileSystemEventHandler = object  # type: ignore


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Plugin protocol (duck-typed)
# ---------------------------------------------------------------------------


class Plugin:
    """Base class plugins can extend (optional — duck-typing is also fine)."""

    name: str = ""

    async def setup(self, app: Any) -> None:  # noqa: D401
        """Install this plugin into the given app."""

    async def teardown(self) -> None:
        """Undo whatever setup() did."""


@dataclass
class PluginRecord:
    name: str
    instance: Plugin
    source_path: Optional[str] = None  # file path or dotted path
    module_name: Optional[str] = None
    loaded_at: float = field(default_factory=time.time)
    reload_count: int = 0


# ---------------------------------------------------------------------------
# Loader helpers
# ---------------------------------------------------------------------------


def _load_from_dotted(path: str) -> Plugin:
    """Load ``pkg.mod:ClassName`` or ``pkg.mod``."""
    module_path, _, attr = path.partition(":")
    mod = importlib.import_module(module_path)
    target = mod if not attr else getattr(mod, attr)
    if isinstance(target, type) and issubclass(target, Plugin):
        return target()
    if isinstance(target, Plugin):
        return target
    # If the module defines a top-level ``plugin`` variable that's a Plugin, use it.
    if hasattr(mod, "plugin") and isinstance(mod.plugin, Plugin):
        return mod.plugin
    raise ValueError(f"could not resolve a Plugin from {path!r}")


def _load_from_file(path: str) -> Plugin:
    """Load a plugin from a .py file path."""
    p = Path(path).resolve()
    if not p.exists():
        raise FileNotFoundError(path)
    spec = importlib.util.spec_from_file_location(f"_mcp_plugin_{p.stem}_{int(time.time())}", p)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load spec from {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    # 1. Explicit `plugin = MyPlugin()` instance at module top-level
    if hasattr(mod, "plugin") and isinstance(mod.plugin, Plugin):
        return mod.plugin
    # 2. A user-defined Plugin subclass (skip the imported base Plugin class itself).
    candidates = []
    for v in vars(mod).values():
        if isinstance(v, type) and issubclass(v, Plugin) and v is not Plugin:
            candidates.append(v)
    if candidates:
        # Prefer a user-defined subclass over the imported base class.
        # If multiple exist, pick the one whose __module__ matches our loaded module.
        local = [c for c in candidates if c.__module__ == mod.__name__]
        chosen = local[0] if local else candidates[0]
        return chosen()
    raise ValueError(f"no Plugin subclass found in {path!r}")


def load_plugin(path: str) -> Plugin:
    if path.endswith(".py") or os.path.sep in path:
        return _load_from_file(path)
    return _load_from_dotted(path)


# ---------------------------------------------------------------------------
# Plugin manager
# ---------------------------------------------------------------------------


class PluginManager:
    """
    Async plugin manager with optional filesystem-driven hot-reload.

    Usage::

        pm = PluginManager(app=my_mcp_app)
        await pm.load_plugin("mcp_agent.enhancements.examples:HelloPlugin")
        await pm.start_watcher()  # optional: enables hot-reload
        # ...later:
        await pm.hot_reload("HelloPlugin")
    """

    def __init__(
        self,
        app: Any = None,
        *,
        hot_reload_enabled: bool = True,
        poll_interval_s: float = 1.0,
        logger: Optional[logging.Logger] = None,
    ):
        self._app = app
        self._plugins: Dict[str, PluginRecord] = {}
        self._hot_reload_enabled = hot_reload_enabled
        # Watchdog is used when available; otherwise we fall back to polling.
        # Tests can force the polling path by setting `_use_watchdog = False`.
        self._use_watchdog = _HAVE_WATCHDOG
        self._poll_interval_s = poll_interval_s
        self._log = logger or logging.getLogger(__name__)
        self._observer: Optional[Any] = None
        self._poller_task: Optional[asyncio.Task] = None
        self._file_mtimes: Dict[str, float] = {}

    # -- introspection ------------------------------------------------------

    @property
    def app(self) -> Any:
        return self._app

    def list_plugins(self) -> List[str]:
        return list(self._plugins.keys())

    def get_plugin(self, name: str) -> Optional[Plugin]:
        rec = self._plugins.get(name)
        return rec.instance if rec else None

    def stats(self) -> Dict[str, Any]:
        return {
            "plugin_count": len(self._plugins),
            "plugins": [
                {
                    "name": r.name,
                    "source_path": r.source_path,
                    "loaded_at": r.loaded_at,
                    "reload_count": r.reload_count,
                }
                for r in self._plugins.values()
            ],
            "hot_reload_enabled": self._hot_reload_enabled,
        }

    # -- lifecycle ---------------------------------------------------------

    async def load_plugin(self, path: str, name: Optional[str] = None) -> Plugin:
        plugin = load_plugin(path)
        plugin_name = name or plugin.name or path
        if plugin_name in self._plugins:
            raise ValueError(f"plugin {plugin_name!r} already loaded")
        record = PluginRecord(
            name=plugin_name,
            instance=plugin,
            source_path=path,
            module_name=getattr(plugin.__class__, "__module__", None),
        )
        await plugin.setup(self._app)
        self._plugins[plugin_name] = record
        if path.endswith(".py") or os.path.sep in path:
            try:
                with open(path, "rb") as f:
                    self._file_mtimes[path] = hash(f.read())
            except OSError:
                pass
        self._log.info("Loaded plugin %s from %s", plugin_name, path)
        return plugin

    async def unload_plugin(self, name: str) -> None:
        rec = self._plugins.pop(name, None)
        if rec is None:
            raise KeyError(name)
        try:
            await rec.instance.teardown()
        except Exception:
            self._log.exception("teardown failed for plugin %s", name)
        self._log.info("Unloaded plugin %s", name)

    async def hot_reload(self, name: str) -> Plugin:
        """
        Re-import the underlying module for ``name`` and swap in a new instance.
        """
        rec = self._plugins.get(name)
        if rec is None:
            raise KeyError(name)
        if rec.source_path is None:
            raise RuntimeError(f"plugin {name!r} has no source_path; cannot hot_reload")
        # teardown old
        try:
            await rec.instance.teardown()
        except Exception:
            self._log.exception("teardown during hot-reload failed for %s", name)
        # reload module
        if rec.source_path.endswith(".py") or os.path.sep in rec.source_path:
            # drop cached module so the next import re-reads the file
            for k in list(sys.modules):
                if rec.source_path in (getattr(sys.modules[k], "__file__", "") or ""):
                    sys.modules.pop(k, None)
        else:
            module_path = rec.source_path.split(":")[0]
            sys.modules.pop(module_path, None)
            # also pop nested submodules so reload picks up changes
            for k in list(sys.modules):
                if k == module_path or k.startswith(module_path + "."):
                    sys.modules.pop(k, None)

        plugin = load_plugin(rec.source_path)
        await plugin.setup(self._app)
        rec.instance = plugin
        rec.reload_count += 1
        rec.loaded_at = time.time()
        self._log.info("Hot-reloaded plugin %s (reload #%d)", name, rec.reload_count)
        return plugin

    # -- watcher -----------------------------------------------------------

    async def start_watcher(self) -> None:
        """Start the filesystem watcher (or the polling fallback)."""
        if not self._hot_reload_enabled:
            self._log.warning("Hot-reload is disabled; start_watcher() is a no-op")
            return
        if self._use_watchdog:
            self._start_watchdog()
        else:
            self._poller_task = asyncio.create_task(self._poll_loop())

    async def stop_watcher(self) -> None:
        if self._observer is not None:
            self._observer.stop()
            self._observer.join(timeout=2.0)
            self._observer = None
        if self._poller_task is not None:
            self._poller_task.cancel()
            try:
                await self._poller_task
            except (asyncio.CancelledError, Exception):
                pass
            self._poller_task = None

    def _start_watchdog(self) -> None:
        if not _HAVE_WATCHDOG:
            return
        # group plugins by parent directory
        dirs: Dict[str, set] = {}
        for rec in self._plugins.values():
            if rec.source_path and (rec.source_path.endswith(".py") or os.path.sep in rec.source_path):
                d = os.path.dirname(rec.source_path) or "."
                dirs.setdefault(d, set()).add(os.path.basename(rec.source_path))

        handler = _PluginWatchHandler(self, dirs)
        self._observer = Observer()
        for d in dirs:
            self._observer.schedule(handler, d, recursive=False)
        self._observer.daemon = True
        self._observer.start()

    async def _poll_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._poll_interval_s)
                for name, rec in list(self._plugins.items()):
                    path = rec.source_path or ""
                    if not path:
                        continue
                    try:
                        with open(path, "rb") as f:
                            blob = f.read()
                    except OSError:
                        continue
                    sig = hash(blob)
                    last = self._file_mtimes.get(path)
                    if last is None:
                        self._file_mtimes[path] = sig
                        continue
                    if sig != last:
                        self._file_mtimes[path] = sig
                        self._log.info("Detected change in %s; hot-reloading %s", path, name)
                        try:
                            await self.hot_reload(name)
                        except Exception:
                            self._log.exception("hot-reload failed for %s", name)
        except asyncio.CancelledError:
            return


if _HAVE_WATCHDOG:

    class _PluginWatchHandler(FileSystemEventHandler):  # type: ignore
        def __init__(self, manager: "PluginManager", dirs: Dict[str, set]):
            self._manager = manager
            self._dirs = dirs
            self._debounce: Dict[str, float] = {}

        def on_modified(self, event):  # noqa: D401
            if event.is_directory:
                return
            src = os.path.abspath(event.src_path)
            for d, files in self._dirs.items():
                d_abs = os.path.abspath(d)
                if os.path.dirname(src) != d_abs:
                    continue
                if os.path.basename(src) not in files:
                    continue
                # debounce: 250ms
                now = time.time()
                if now - self._debounce.get(src, 0.0) < 0.25:
                    return
                self._debounce[src] = now
                # find the plugin name(s) using this file
                for name, rec in list(self._manager._plugins.items()):
                    if rec.source_path and os.path.abspath(rec.source_path) == src:
                        try:
                            asyncio.ensure_future(self._manager.hot_reload(name))
                        except Exception:
                            self._manager._log.exception("hot-reload scheduling failed")
else:
    class _PluginWatchHandler:  # pragma: no cover
        pass


__all__ = [
    "Plugin",
    "PluginRecord",
    "PluginManager",
    "load_plugin",
]
