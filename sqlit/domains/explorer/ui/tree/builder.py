"""Tree construction helpers for explorer tree mixins."""

from __future__ import annotations

from typing import Any, Callable

from rich.markup import escape as escape_markup

from sqlit.domains.connections.providers.metadata import get_connection_display_info
from sqlit.domains.explorer.domain.tree_nodes import ConnectionNode, FolderNode
from sqlit.shared.ui.protocols import TreeMixinHost

MIN_TIMER_DELAY_S = 0.001
POPULATE_CONNECTED_DEFER_S = 0.15
MAX_SYNC_CONNECTIONS = 50


def _sort_connections_for_display(connections: list[Any]) -> list[Any]:
    return [
        conn
        for _, conn in sorted(
            enumerate(connections),
            key=lambda item: (not getattr(item[1], "favorite", False), item[0]),
        )
    ]


def _find_connection_node(host: TreeMixinHost, config: Any) -> Any | None:
    for node in host.object_tree.root.children:
        if host._get_node_kind(node) != "connection":
            continue
        data = getattr(node, "data", None)
        node_config = getattr(data, "config", None)
        if node_config and node_config.name == config.name:
            return node
    return None


def ensure_connecting_indicator(host: TreeMixinHost, config: Any) -> None:
    """Ensure a connecting node exists without rebuilding the tree."""
    spinner = host._connect_spinner_frame()
    label = host._format_connection_label(config, "connecting", spinner=spinner)
    node = _find_connection_node(host, config)
    if node is not None:
        node.set_label(label)
        node.allow_expand = False
        return
    node = host.object_tree.root.add(label)
    node.data = ConnectionNode(config=config)
    node.allow_expand = False


def clear_connecting_indicator(host: TreeMixinHost, config: Any | None) -> None:
    """Clear connecting state without rebuilding the tree."""
    if config is None:
        return
    node = _find_connection_node(host, config)
    if node is None:
        return
    is_saved = any(c.name == config.name for c in host.connections)
    if host.current_config and host.current_config.name == config.name:
        label = host._format_connection_label(config, "connected")
        node.set_label(label)
        node.allow_expand = True
        return
    if is_saved:
        label = host._format_connection_label(config, "idle")
        node.set_label(label)
        node.allow_expand = False
        return
    try:
        node.remove()
    except Exception:
        pass


def schedule_populate_connected_tree(
    host: TreeMixinHost,
    *,
    on_done: Callable[[], None] | None = None,
) -> None:
    """Populate connected tree via idle scheduler with a timed fallback."""
    populate_token = object()
    setattr(host, "_populate_connected_token", populate_token)

    def populate_once() -> None:
        if getattr(host, "_populate_connected_token", None) is not populate_token:
            return
        setattr(host, "_populate_connected_token", None)
        populate_connected_tree(host)
        if on_done:
            on_done()

    try:
        from sqlit.domains.shell.app.idle_scheduler import Priority, get_idle_scheduler
    except Exception:
        scheduler = None
    else:
        scheduler = get_idle_scheduler()
    if scheduler:
        scheduler.cancel_all(name="populate-connected-tree")
        scheduler.request_idle_callback(
            populate_once,
            priority=Priority.HIGH,
            name="populate-connected-tree",
        )
    else:
        host.set_timer(MIN_TIMER_DELAY_S, populate_once)
    host.set_timer(POPULATE_CONNECTED_DEFER_S, populate_once)


def update_connecting_indicator(host: TreeMixinHost) -> None:
    connecting_config = getattr(host, "_connecting_config", None)
    if not connecting_config:
        return

    spinner = host._connect_spinner_frame()
    label = host._format_connection_label(connecting_config, "connecting", spinner=spinner)

    for node in host.object_tree.root.children:
        if host._get_node_kind(node) != "connection":
            continue
        data = getattr(node, "data", None)
        config = getattr(data, "config", None)
        if config and config.name == connecting_config.name:
            node.set_label(label)
            node.allow_expand = False
            break


def refresh_tree(host: TreeMixinHost) -> None:
    """Refresh the explorer tree."""
    host.object_tree.clear()
    host.object_tree.root.expand()

    connecting_config = getattr(host, "_connecting_config", None)
    connecting_name = connecting_config.name if connecting_config else None
    connecting_spinner = host._connect_spinner_frame() if connecting_config else None

    direct_config = getattr(host, "_direct_connection_config", None)
    direct_active = (
        direct_config is not None
        and host.current_config is not None
        and direct_config.name == host.current_config.name
    )
    if direct_active and host.current_config is not None:
        connections = [host.current_config]
    else:
        connections = list(host.connections)
    if connecting_config and not any(c.name == connecting_config.name for c in connections):
        connections = connections + [connecting_config]
    connections = _sort_connections_for_display(connections)

    for conn in connections:
        is_connected = host.current_config is not None and conn.name == host.current_config.name
        is_connecting = connecting_name == conn.name and not is_connected
        if is_connected:
            label = host._format_connection_label(conn, "connected")
        elif is_connecting:
            label = host._format_connection_label(conn, "connecting", spinner=connecting_spinner)
        else:
            label = host._format_connection_label(conn, "idle")
        node = host.object_tree.root.add(label)
        node.data = ConnectionNode(config=conn)
        node.allow_expand = is_connected

    if host.current_connection is not None and host.current_config is not None:
        populate_connected_tree(host)


def refresh_tree_chunked(
    host: TreeMixinHost,
    *,
    batch_size: int = 10,
    on_done: Callable[[], None] | None = None,
) -> None:
    """Refresh the explorer tree in small batches to reduce UI stalls."""
    token = object()
    setattr(host, "_tree_refresh_token", token)

    host.object_tree.clear()
    host.object_tree.root.expand()

    connecting_config = getattr(host, "_connecting_config", None)
    connecting_name = connecting_config.name if connecting_config else None
    connecting_spinner = host._connect_spinner_frame() if connecting_config else None

    direct_config = getattr(host, "_direct_connection_config", None)
    direct_active = (
        direct_config is not None
        and host.current_config is not None
        and direct_config.name == host.current_config.name
    )
    if direct_active and host.current_config is not None:
        connections = [host.current_config]
    else:
        connections = list(host.connections)
    if connecting_config and not any(c.name == connecting_config.name for c in connections):
        connections = connections + [connecting_config]
    connections = _sort_connections_for_display(connections)

    def schedule_populate() -> None:
        if getattr(host, "_tree_refresh_token", None) is not token:
            return

        def populate_and_done() -> None:
            if getattr(host, "_tree_refresh_token", None) is not token:
                return
            if host.current_connection is not None and host.current_config is not None:
                populate_connected_tree(host)
            if on_done:
                on_done()

        if host.current_connection is not None and host.current_config is not None:
            populate_token = object()
            setattr(host, "_populate_connected_token", populate_token)

            def populate_once() -> None:
                if getattr(host, "_tree_refresh_token", None) is not token:
                    return
                if getattr(host, "_populate_connected_token", None) is not populate_token:
                    return
                setattr(host, "_populate_connected_token", None)
                populate_and_done()

            try:
                from sqlit.domains.shell.app.idle_scheduler import (
                    Priority,
                    get_idle_scheduler,
                )
            except Exception:
                scheduler = None
            else:
                scheduler = get_idle_scheduler()
            if scheduler:
                scheduler.cancel_all(name="populate-connected-tree")
                scheduler.request_idle_callback(
                    populate_once,
                    priority=Priority.HIGH,
                    name="populate-connected-tree",
                )
            else:
                host.set_timer(MIN_TIMER_DELAY_S, populate_once)
            host.set_timer(POPULATE_CONNECTED_DEFER_S, populate_once)
        else:
            if on_done:
                on_done()

    if len(connections) <= MAX_SYNC_CONNECTIONS:
        for conn in connections:
            is_connected = host.current_config is not None and conn.name == host.current_config.name
            is_connecting = connecting_name == conn.name and not is_connected
            if is_connected:
                label = host._format_connection_label(conn, "connected")
            elif is_connecting:
                label = host._format_connection_label(conn, "connecting", spinner=connecting_spinner)
            else:
                label = host._format_connection_label(conn, "idle")
            node = host.object_tree.root.add(label)
            node.data = ConnectionNode(config=conn)
            node.allow_expand = is_connected

        def finish_sync() -> None:
            schedule_populate()

        host.set_timer(MIN_TIMER_DELAY_S, finish_sync)
        return

    batch_size = max(1, int(batch_size))
    idx = 0

    def add_batch() -> None:
        nonlocal idx
        if getattr(host, "_tree_refresh_token", None) is not token:
            return
        end = min(idx + batch_size, len(connections))
        for conn in connections[idx:end]:
            is_connected = host.current_config is not None and conn.name == host.current_config.name
            is_connecting = connecting_name == conn.name and not is_connected
            if is_connected:
                label = host._format_connection_label(conn, "connected")
            elif is_connecting:
                label = host._format_connection_label(conn, "connecting", spinner=connecting_spinner)
            else:
                label = host._format_connection_label(conn, "idle")
            node = host.object_tree.root.add(label)
            node.data = ConnectionNode(config=conn)
            node.allow_expand = is_connected
        idx = end
        if idx < len(connections):
            host.set_timer(MIN_TIMER_DELAY_S, add_batch)
            return

        def finish() -> None:
            schedule_populate()

        host.set_timer(MIN_TIMER_DELAY_S, finish)

    add_batch()


def populate_connected_tree(host: TreeMixinHost) -> None:
    """Populate tree with database objects when connected."""
    if (
        host.current_connection is None
        or host.current_config is None
        or host.current_provider is None
    ):
        return

    provider = host.current_provider
    def get_conn_label(config: Any, connected: bool = False) -> str:
        display_info = escape_markup(get_connection_display_info(config))
        db_type_label = host._db_type_badge(config.db_type)
        escaped_name = escape_markup(config.name)
        source_emoji = config.get_source_emoji() if hasattr(config, "get_source_emoji") else ""
        favorite_prefix = "[bright_yellow]*[/] " if getattr(config, "favorite", False) else ""
        if connected:
            name = f"{favorite_prefix}[#4ADE80]• {source_emoji}{escaped_name}[/]"
        else:
            name = f"{favorite_prefix}{source_emoji}{escaped_name}"
        return f"{name} [{db_type_label}] ({display_info})"

    active_node = None
    for child in host.object_tree.root.children:
        if host._get_node_kind(child) == "connection":
            data = getattr(child, "data", None)
            config = getattr(data, "config", None)
            if config and config.name == host.current_config.name:
                child.set_label(get_conn_label(host.current_config, connected=True))
                active_node = child
                break

    if not active_node:
        active_node = host.object_tree.root.add(get_conn_label(host.current_config, connected=True))
        active_node.data = ConnectionNode(config=host.current_config)
        active_node.allow_expand = True

    active_node.remove_children()

    try:
        if provider.capabilities.supports_multiple_databases:
            endpoint = host.current_config.tcp_endpoint
            specific_db = endpoint.database if endpoint else ""
            show_single_db = specific_db and specific_db.lower() not in ("", "master")
            if show_single_db:
                add_database_object_nodes(host, active_node, specific_db)
                active_node.expand()
            else:
                dbs_node = active_node.add("Databases")
                dbs_node.data = FolderNode(folder_type="databases")
                dbs_node.allow_expand = True
                active_node.expand()
        else:
            add_database_object_nodes(host, active_node, None)
            active_node.expand()

    except Exception as error:
        host.notify(f"Error loading objects: {error}", severity="error")


def add_database_object_nodes(host: TreeMixinHost, parent_node: Any, database: str | None) -> None:
    """Add Tables, Views, Indexes, Triggers, Sequences, and Stored Procedures nodes."""
    if not host.current_provider:
        return

    caps = host.current_provider.capabilities
    node_provider = host.current_provider.explorer_nodes

    for folder in node_provider.get_root_folders(caps):
        if folder.requires(caps):
            folder_node = parent_node.add(folder.label)
            folder_node.data = FolderNode(folder_type=folder.kind, database=database)
            folder_node.allow_expand = True
        else:
            parent_node.add_leaf(f"[dim]{folder.label} (Not available)[/]")
