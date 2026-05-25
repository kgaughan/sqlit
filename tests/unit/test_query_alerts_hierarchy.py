"""Tests for the alert mode resolution hierarchy and CLI/shell helpers."""

from __future__ import annotations

import argparse

from sqlit.domains.query.app.alerts import (
    AlertMode,
    lookup_database_override,
    make_db_alert_key,
    resolve_alert_mode,
)


def test_resolve_falls_back_to_global() -> None:
    mode, source = resolve_alert_mode(
        global_mode="delete",
        connection_option=None,
        database_override=None,
    )
    assert mode == AlertMode.DELETE
    assert source == "global"


def test_resolve_connection_overrides_global() -> None:
    mode, source = resolve_alert_mode(
        global_mode="off",
        connection_option="write",
        database_override=None,
    )
    assert mode == AlertMode.WRITE
    assert source == "connection"


def test_resolve_database_overrides_everything() -> None:
    mode, source = resolve_alert_mode(
        global_mode="write",
        connection_option="delete",
        database_override="off",
    )
    assert mode == AlertMode.OFF
    assert source == "database"


def test_resolve_invalid_values_fall_through() -> None:
    # Invalid db override should fall through to connection.
    mode, source = resolve_alert_mode(
        global_mode="off",
        connection_option="delete",
        database_override="not-a-mode",
    )
    assert mode == AlertMode.DELETE
    assert source == "connection"


def test_resolve_default_off_when_nothing_set() -> None:
    mode, source = resolve_alert_mode()
    assert mode == AlertMode.OFF
    assert source == "global"


def test_lookup_database_override_key() -> None:
    overrides = {make_db_alert_key("MyConn", "warehouse"): "delete"}
    assert lookup_database_override(overrides, "MyConn", "warehouse") == "delete"
    assert lookup_database_override(overrides, "MyConn", "other") is None
    assert lookup_database_override(overrides, None, "warehouse") is None
    assert lookup_database_override(overrides, "MyConn", None) is None
    assert lookup_database_override(None, "MyConn", "warehouse") is None


def test_apply_alert_option_sets_and_clears() -> None:
    from sqlit.domains.connections.cli.commands import _apply_alert_option
    from sqlit.domains.connections.domain.config import ConnectionConfig
    from sqlit.domains.query.app.alerts import CONNECTION_ALERT_OPTION

    config = ConnectionConfig(name="x")
    assert _apply_alert_option(config, "delete") is None
    assert config.get_option(CONNECTION_ALERT_OPTION) == "delete"

    assert _apply_alert_option(config, "unset") is None
    assert CONNECTION_ALERT_OPTION not in config.options

    err = _apply_alert_option(config, "garbage")
    assert err is not None
    assert "Invalid --alert" in err


def _make_services(settings: dict | None = None):
    from sqlit.shared.app.runtime import RuntimeConfig
    from tests.ui.mocks import (
        MockConnectionStore,
        MockHistoryStore,
        MockSettingsStore,
        build_test_services,
    )

    return build_test_services(
        runtime=RuntimeConfig(),
        connection_store=MockConnectionStore(),
        settings_store=MockSettingsStore(settings),
        history_store=MockHistoryStore(),
    )


def test_alerts_cli_set_global_and_list(capsys) -> None:
    from sqlit.domains.query.cli.alerts_commands import (
        cmd_alerts_list,
        cmd_alerts_set,
    )

    services = _make_services()
    rc = cmd_alerts_set(
        argparse.Namespace(mode="write", connection=None, database=None),
        services=services,
    )
    assert rc == 0
    assert services.settings_store.get("query_alert_mode") == int(AlertMode.WRITE)

    cmd_alerts_list(argparse.Namespace(), services=services)
    output = capsys.readouterr().out
    assert "Global: write" in output


def test_alerts_cli_set_database_requires_connection() -> None:
    from sqlit.domains.query.cli.alerts_commands import cmd_alerts_set

    services = _make_services()
    rc = cmd_alerts_set(
        argparse.Namespace(mode="delete", connection=None, database="prod"),
        services=services,
    )
    assert rc == 1


def test_alerts_cli_set_connection_persists_option() -> None:
    from sqlit.domains.connections.domain.config import ConnectionConfig
    from sqlit.domains.query.app.alerts import CONNECTION_ALERT_OPTION
    from sqlit.domains.query.cli.alerts_commands import cmd_alerts_set, cmd_alerts_unset

    services = _make_services()
    services.connection_store.save_all([ConnectionConfig(name="prod")])

    rc = cmd_alerts_set(
        argparse.Namespace(mode="delete", connection="prod", database=None),
        services=services,
    )
    assert rc == 0
    saved = services.connection_store.load_all(load_credentials=False)
    assert saved[0].get_option(CONNECTION_ALERT_OPTION) == "delete"

    rc = cmd_alerts_unset(
        argparse.Namespace(connection="prod", database=None),
        services=services,
    )
    assert rc == 0
    saved = services.connection_store.load_all(load_credentials=False)
    assert CONNECTION_ALERT_OPTION not in saved[0].options


def test_alerts_cli_database_override_roundtrip() -> None:
    from sqlit.domains.connections.domain.config import ConnectionConfig
    from sqlit.domains.query.app.alerts import (
        DATABASE_ALERT_SETTING,
        make_db_alert_key,
    )
    from sqlit.domains.query.cli.alerts_commands import cmd_alerts_set, cmd_alerts_unset

    services = _make_services()
    services.connection_store.save_all([ConnectionConfig(name="prod")])

    rc = cmd_alerts_set(
        argparse.Namespace(mode="write", connection="prod", database="warehouse"),
        services=services,
    )
    assert rc == 0
    overrides = services.settings_store.get(DATABASE_ALERT_SETTING)
    assert overrides == {make_db_alert_key("prod", "warehouse"): "write"}

    rc = cmd_alerts_unset(
        argparse.Namespace(connection="prod", database="warehouse"),
        services=services,
    )
    assert rc == 0
    overrides = services.settings_store.get(DATABASE_ALERT_SETTING)
    assert make_db_alert_key("prod", "warehouse") not in overrides
