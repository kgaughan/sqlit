"""UI tests for the ConnectionScreen."""

from __future__ import annotations

import pytest

from sqlit.config import ConnectionConfig
from sqlit.db.schema import get_all_schemas

from .conftest import ConnectionScreenTestApp


def _get_providers_with_advanced_tab() -> set[str]:
    return {
        db_type
        for db_type, schema in get_all_schemas().items()
        if any(f.advanced for f in schema.fields)
    }


def _get_providers_without_advanced_tab() -> set[str]:
    return {
        db_type
        for db_type, schema in get_all_schemas().items()
        if not any(f.advanced for f in schema.fields)
    }


class TestConnectionScreen:
    @pytest.mark.asyncio
    async def test_create_connection(self):
        app = ConnectionScreenTestApp()

        async with app.run_test(size=(100, 35)) as pilot:
            screen = app.screen
            screen.query_one("#conn-name").value = "my-mssql"
            screen.query_one("#field-server").value = "localhost"
            screen.query_one("#field-port").value = "1433"
            screen.query_one("#field-database").value = "mydb"
            screen.query_one("#field-username").value = "sa"
            screen.query_one("#field-password").value = "secret"

            screen.action_save()
            await pilot.pause()

        assert app.screen_result is not None
        action, config = app.screen_result
        assert action == "save"
        assert config.name == "my-mssql"
        assert config.db_type == "mssql"
        assert config.server == "localhost"
        assert config.port == "1433"
        assert config.database == "mydb"
        assert config.username == "sa"
        assert config.password == "secret"

    @pytest.mark.asyncio
    async def test_edit_connection(self):
        original = ConnectionConfig(
            name="prod-db",
            db_type="mssql",
            server="old-server",
            port="1433",
            database="olddb",
            username="olduser",
            password="oldpass",
        )
        app = ConnectionScreenTestApp(original, editing=True)

        async with app.run_test(size=(100, 35)) as pilot:
            screen = app.screen
            assert screen.query_one("#conn-name").value == "prod-db"
            assert screen.query_one("#field-server").value == "old-server"

            screen.query_one("#conn-name").value = "new-prod-db"
            screen.query_one("#field-server").value = "new-server"
            screen.query_one("#field-database").value = "newdb"

            screen.action_save()
            await pilot.pause()

        assert app.screen_result is not None
        action, config = app.screen_result
        assert action == "save"
        assert config.name == "new-prod-db"
        assert config.db_type == "mssql"
        assert config.server == "new-server"
        assert config.database == "newdb"

    @pytest.mark.asyncio
    async def test_cancel_connection(self):
        app = ConnectionScreenTestApp()

        async with app.run_test(size=(100, 35)) as pilot:
            screen = app.screen
            screen.action_cancel()
            await pilot.pause()

        assert app.screen_result is None

    @pytest.mark.asyncio
    async def test_empty_fields_shows_validation_errors(self):
        app = ConnectionScreenTestApp()

        async with app.run_test(size=(100, 35)) as pilot:
            screen = app.screen

            screen.action_save()
            await pilot.pause()

            assert not screen.validation_state.is_valid()
            assert screen.validation_state.has_error("server")
            assert screen.validation_state.has_error("username")

            container_server = screen.query_one("#container-server")
            container_username = screen.query_one("#container-username")
            assert "invalid" in container_server.classes
            assert "invalid" in container_username.classes

            screen.query_one("#field-server").value = "localhost"
            screen.action_save()
            await pilot.pause()

            assert screen.validation_state.has_error("username")
            assert not screen.validation_state.has_error("server")

        assert app.screen_result is None

    @pytest.mark.asyncio
    async def test_save_from_ssh_tab_marks_general_tab_with_error(self):
        app = ConnectionScreenTestApp()

        async with app.run_test(size=(100, 35)) as pilot:
            screen = app.screen
            tabs = screen.query_one("#connection-tabs")
            tabs.active = "tab-ssh"
            await pilot.pause()

            screen.action_save()
            await pilot.pause()

            assert screen.validation_state.has_tab_error("tab-general")

    @pytest.mark.asyncio
    async def test_save_from_ssh_tab_redirects_to_general_on_error(self):
        app = ConnectionScreenTestApp()

        async with app.run_test(size=(100, 35)) as pilot:
            screen = app.screen
            tabs = screen.query_one("#connection-tabs")
            tabs.active = "tab-ssh"
            await pilot.pause()

            screen.action_save()
            await pilot.pause()

            assert tabs.active == "tab-general"


class TestAdvancedTab:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("db_type", _get_providers_with_advanced_tab())
    async def test_advanced_tab_enabled(self, db_type):
        config = ConnectionConfig(name="test", db_type=db_type)
        app = ConnectionScreenTestApp(config, editing=True)

        async with app.run_test(size=(100, 35)) as pilot:
            screen = app.screen
            tabs = screen.query_one("#connection-tabs")
            advanced_pane = screen.query_one("#tab-advanced")
            advanced_tab = tabs.get_tab(advanced_pane)

            assert not advanced_tab.disabled

    @pytest.mark.asyncio
    @pytest.mark.parametrize("db_type", _get_providers_without_advanced_tab())
    async def test_advanced_tab_disabled(self, db_type):
        config = ConnectionConfig(name="test", db_type=db_type)
        app = ConnectionScreenTestApp(config, editing=True)

        async with app.run_test(size=(100, 35)) as pilot:
            screen = app.screen
            tabs = screen.query_one("#connection-tabs")
            advanced_pane = screen.query_one("#tab-advanced")
            advanced_tab = tabs.get_tab(advanced_pane)

            assert advanced_tab.disabled
