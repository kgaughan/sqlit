"""Modal screens for sqlit."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal
from textual.screen import ModalScreen
from textual.widgets import DataTable, Input, OptionList, Static
from textual.widgets.option_list import Option

from .config import AUTH_TYPE_LABELS, AuthType, ConnectionConfig


class ConfirmScreen(ModalScreen):
    """Modal screen for confirmation dialogs."""

    BINDINGS = [
        Binding("y", "confirm", "Yes"),
        Binding("n", "cancel", "No"),
        Binding("escape", "cancel", "Cancel"),
        Binding("enter", "select_option", "Select"),
    ]

    CSS = """
    ConfirmScreen {
        align: center middle;
        background: transparent;
    }

    #confirm-dialog {
        width: 30;
        height: auto;
        border: solid $primary;
        background: $surface;
        padding: 1 2;
    }

    #confirm-title {
        text-align: center;
        text-style: bold;
        margin-bottom: 1;
    }

    #confirm-list {
        height: auto;
        background: $surface;
        border: none;
        padding: 0;
    }

    #confirm-list > .option-list--option {
        padding: 0;
    }
    """

    def __init__(self, title: str):
        super().__init__()
        self.title_text = title

    def compose(self) -> ComposeResult:
        with Container(id="confirm-dialog"):
            yield Static(self.title_text, id="confirm-title")
            option_list = OptionList(
                Option(r"\[Y] Yes", id="yes"),
                Option(r"\[N] No", id="no"),
                id="confirm-list",
            )
            yield option_list

    def on_mount(self) -> None:
        self.query_one("#confirm-list", OptionList).focus()

    def on_option_list_option_selected(self, event) -> None:
        self.dismiss(event.option.id == "yes")

    def action_select_option(self) -> None:
        option_list = self.query_one("#confirm-list", OptionList)
        if option_list.highlighted is not None:
            self.dismiss(
                option_list.get_option_at_index(option_list.highlighted).id == "yes"
            )

    def action_confirm(self) -> None:
        self.dismiss(True)

    def action_cancel(self) -> None:
        self.dismiss(False)


class ConnectionScreen(ModalScreen):
    """Modal screen for adding/editing a connection."""

    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("ctrl+s", "save", "Save", priority=True),
        Binding("ctrl+t", "test_connection", "Test", priority=True),
        Binding("tab", "next_field", "Next field", priority=True),
        Binding("shift+tab", "prev_field", "Previous field", priority=True),
    ]

    CSS = """
    ConnectionScreen {
        align: center middle;
        background: transparent;
    }

    #connection-dialog {
        width: 70;
        height: auto;
        border: solid $primary;
        background: $surface;
        padding: 1 2;
    }

    #connection-title {
        text-align: center;
        text-style: bold;
        margin-bottom: 1;
    }

    .field-label {
        margin-top: 1;
        color: $text-muted;
    }

    .field-label:first-of-type {
        margin-top: 0;
    }

    #connection-dialog Input {
        margin-bottom: 0;
    }

    #server-port-row {
        height: auto;
        width: 100%;
    }

    #server-field {
        width: 1fr;
        height: auto;
    }

    #port-field {
        width: 12;
        height: auto;
        margin-left: 1;
    }

    #server-field .field-label,
    #port-field .field-label {
        margin-top: 1;
    }

    #driver-list {
        height: auto;
        max-height: 4;
        background: $surface;
        border: solid $primary-darken-2;
        padding: 0;
        margin-bottom: 0;
    }

    #driver-list > .option-list--option {
        padding: 0 1;
    }

    #auth-list {
        height: auto;
        max-height: 7;
        background: $surface;
        border: solid $primary-darken-2;
        padding: 0;
        margin-bottom: 0;
    }

    #auth-list > .option-list--option {
        padding: 0 1;
    }

    #username-field {
        height: auto;
    }

    #username-field.hidden {
        display: none;
    }

    #password-field {
        height: auto;
    }

    #password-field.hidden {
        display: none;
    }
    """

    AUTH_NEEDS_USERNAME = {AuthType.SQL_SERVER, AuthType.AD_PASSWORD, AuthType.AD_INTERACTIVE}
    AUTH_NEEDS_PASSWORD = {AuthType.SQL_SERVER, AuthType.AD_PASSWORD}

    def __init__(self, config: ConnectionConfig | None = None, editing: bool = False):
        super().__init__()
        self.config = config
        self.editing = editing

    def _get_initial_auth_type(self) -> AuthType:
        """Get the initial auth type from config."""
        if self.config:
            return self.config.get_auth_type()
        return AuthType.SQL_SERVER

    def compose(self) -> ComposeResult:
        title = "Edit Connection" if self.editing else "New Connection"
        auth_type = self._get_initial_auth_type()
        show_username = auth_type in self.AUTH_NEEDS_USERNAME
        show_password = auth_type in self.AUTH_NEEDS_PASSWORD

        with Container(id="connection-dialog"):
            yield Static(title, id="connection-title")

            yield Static("Name", classes="field-label")
            yield Input(
                value=self.config.name if self.config else "",
                placeholder="My Server",
                id="conn-name",
            )

            with Horizontal(id="server-port-row"):
                with Container(id="server-field"):
                    yield Static("Server", classes="field-label")
                    yield Input(
                        value=self.config.server if self.config else "",
                        placeholder="localhost or server\\instance",
                        id="conn-server",
                    )
                with Container(id="port-field"):
                    yield Static("Port", classes="field-label")
                    yield Input(
                        value=self.config.port if self.config else "1433",
                        placeholder="1433",
                        id="conn-port",
                    )

            yield Static("Database (empty = browse all)", classes="field-label")
            yield Input(
                value=self.config.database if self.config else "",
                placeholder="Leave empty to browse all databases",
                id="conn-database",
            )

            yield Static("Driver", classes="field-label")
            from .drivers import get_installed_drivers, SUPPORTED_DRIVERS
            installed = get_installed_drivers()
            driver_options = []
            current_driver = self.config.driver if self.config else "ODBC Driver 18 for SQL Server"
            if installed:
                for driver in installed:
                    driver_options.append(Option(driver, id=driver))
            else:
                # Show supported drivers even if not installed
                for driver in SUPPORTED_DRIVERS[:3]:
                    driver_options.append(Option(f"[dim]{driver}[/]", id=driver))
            yield OptionList(*driver_options, id="driver-list")

            yield Static("Authentication", classes="field-label")
            auth_list = OptionList(
                Option(AUTH_TYPE_LABELS[AuthType.SQL_SERVER], id=AuthType.SQL_SERVER.value),
                Option(AUTH_TYPE_LABELS[AuthType.WINDOWS], id=AuthType.WINDOWS.value),
                Option(AUTH_TYPE_LABELS[AuthType.AD_PASSWORD], id=AuthType.AD_PASSWORD.value),
                Option(
                    AUTH_TYPE_LABELS[AuthType.AD_INTERACTIVE], id=AuthType.AD_INTERACTIVE.value
                ),
                Option(
                    AUTH_TYPE_LABELS[AuthType.AD_INTEGRATED], id=AuthType.AD_INTEGRATED.value
                ),
                id="auth-list",
            )
            yield auth_list

            with Container(
                id="username-field", classes="" if show_username else "hidden"
            ):
                yield Static("Username", classes="field-label")
                yield Input(
                    value=self.config.username if self.config else "",
                    placeholder="user@domain.com",
                    id="conn-username",
                )

            with Container(
                id="password-field", classes="" if show_password else "hidden"
            ):
                yield Static("Password", classes="field-label")
                yield Input(
                    value=self.config.password if self.config else "",
                    id="conn-password",
                )

    def on_mount(self) -> None:
        self.query_one("#conn-name", Input).focus()

        # Set initial driver selection
        driver_list = self.query_one("#driver-list", OptionList)
        current_driver = self.config.driver if self.config else "ODBC Driver 18 for SQL Server"
        for i in range(driver_list.option_count):
            option = driver_list.get_option_at_index(i)
            if option.id == current_driver:
                driver_list.highlighted = i
                break

        # Set initial auth type selection
        auth_list = self.query_one("#auth-list", OptionList)
        auth_type = self._get_initial_auth_type()
        auth_options = [
            AuthType.SQL_SERVER,
            AuthType.WINDOWS,
            AuthType.AD_PASSWORD,
            AuthType.AD_INTERACTIVE,
            AuthType.AD_INTEGRATED,
        ]
        try:
            auth_list.highlighted = auth_options.index(auth_type)
        except ValueError:
            auth_list.highlighted = 0

    def on_option_list_option_highlighted(self, event) -> None:
        if event.option_list.id == "auth-list":
            try:
                auth_type = AuthType(event.option.id)
            except ValueError:
                return

            username_field = self.query_one("#username-field")
            password_field = self.query_one("#password-field")

            if auth_type in self.AUTH_NEEDS_USERNAME:
                username_field.remove_class("hidden")
            else:
                username_field.add_class("hidden")

            if auth_type in self.AUTH_NEEDS_PASSWORD:
                password_field.remove_class("hidden")
            else:
                password_field.add_class("hidden")

    def _get_focusable_fields(self) -> list:
        """Get list of focusable fields in order."""
        fields = [
            self.query_one("#conn-name", Input),
            self.query_one("#conn-server", Input),
            self.query_one("#conn-port", Input),
            self.query_one("#conn-database", Input),
            self.query_one("#driver-list", OptionList),
            self.query_one("#auth-list", OptionList),
        ]
        username_field = self.query_one("#username-field")
        password_field = self.query_one("#password-field")
        if "hidden" not in username_field.classes:
            fields.append(self.query_one("#conn-username", Input))
        if "hidden" not in password_field.classes:
            fields.append(self.query_one("#conn-password", Input))
        return fields

    def action_next_field(self) -> None:
        fields = self._get_focusable_fields()
        focused = self.focused
        if focused in fields:
            idx = fields.index(focused)
            next_idx = (idx + 1) % len(fields)
            fields[next_idx].focus()
        elif fields:
            fields[0].focus()

    def action_prev_field(self) -> None:
        fields = self._get_focusable_fields()
        focused = self.focused
        if focused in fields:
            idx = fields.index(focused)
            prev_idx = (idx - 1) % len(fields)
            fields[prev_idx].focus()
        elif fields:
            fields[-1].focus()

    def _get_selected_driver(self) -> str:
        """Get the currently selected driver."""
        driver_list = self.query_one("#driver-list", OptionList)
        idx = driver_list.highlighted or 0
        if idx < driver_list.option_count:
            return driver_list.get_option_at_index(idx).id
        return "ODBC Driver 18 for SQL Server"

    def _get_selected_auth_type(self) -> AuthType:
        """Get the currently selected auth type."""
        auth_list = self.query_one("#auth-list", OptionList)
        auth_options = [
            AuthType.SQL_SERVER,
            AuthType.WINDOWS,
            AuthType.AD_PASSWORD,
            AuthType.AD_INTERACTIVE,
            AuthType.AD_INTEGRATED,
        ]
        idx = auth_list.highlighted or 0
        return auth_options[idx] if idx < len(auth_options) else AuthType.SQL_SERVER

    def _get_config(self) -> ConnectionConfig | None:
        name = self.query_one("#conn-name", Input).value
        server = self.query_one("#conn-server", Input).value
        port = self.query_one("#conn-port", Input).value or "1433"
        database = self.query_one("#conn-database", Input).value or "master"
        driver = self._get_selected_driver()

        auth_type = self._get_selected_auth_type()

        username = ""
        password = ""

        if auth_type in self.AUTH_NEEDS_USERNAME:
            username = self.query_one("#conn-username", Input).value
        if auth_type in self.AUTH_NEEDS_PASSWORD:
            password = self.query_one("#conn-password", Input).value

        if not name or not server:
            self.notify("Name and Server are required", severity="error")
            return None

        if auth_type in self.AUTH_NEEDS_USERNAME and not username:
            self.notify(
                "Username is required for this authentication type", severity="error"
            )
            return None

        return ConnectionConfig(
            name=name,
            server=server,
            port=port,
            database=database,
            username=username,
            password=password,
            auth_type=auth_type.value,
            driver=driver,
            trusted_connection=(auth_type == AuthType.WINDOWS),
        )

    def action_test_connection(self) -> None:
        """Test the connection without saving or closing."""
        config = self._get_config()
        if not config:
            return

        try:
            import pyodbc

            conn_str = config.get_connection_string()
            conn = pyodbc.connect(conn_str, timeout=10)
            conn.close()
            self.notify("Connection successful!", severity="information")
        except ImportError:
            self.notify("pyodbc not installed", severity="error")
        except Exception as e:
            self.notify(f"Connection failed: {e}", severity="error")

    def action_save(self) -> None:
        config = self._get_config()
        if config:
            self.dismiss(("save", config))

    def action_cancel(self) -> None:
        self.dismiss(None)


class QueryResultScreen(ModalScreen):
    """Modal screen showing query results."""

    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("q", "dismiss", "Close"),
    ]

    CSS = """
    QueryResultScreen {
        align: center middle;
    }

    #result-container {
        width: 95%;
        height: 90%;
        border: thick $accent;
        background: $surface;
        padding: 1 2;
    }

    #result-header {
        height: 3;
        background: $accent;
        padding: 0 1;
        margin-bottom: 1;
    }

    #result-table {
        height: 1fr;
    }

    #result-info {
        height: 3;
        padding: 1;
    }
    """

    def __init__(self, columns: list[str], rows: list[tuple], row_count: int):
        super().__init__()
        self.columns = columns
        self.rows = rows
        self.row_count = row_count

    def compose(self) -> ComposeResult:
        with Container(id="result-container"):
            yield Static("Query Results", id="result-header")

            table = DataTable(id="result-table")
            table.add_columns(*self.columns)
            yield table

            yield Static(
                f"Showing {len(self.rows)} of {self.row_count} rows",
                id="result-info",
            )

    def on_mount(self) -> None:
        table = self.query_one("#result-table", DataTable)
        for row in self.rows:
            str_row = tuple(str(v) if v is not None else "NULL" for v in row)
            table.add_row(*str_row)

    def action_dismiss(self) -> None:
        self.dismiss(None)


class DriverSetupScreen(ModalScreen):
    """Screen for setting up ODBC drivers."""

    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("enter", "select", "Select"),
        Binding("i", "install_driver", "Install"),
    ]

    CSS = """
    DriverSetupScreen {
        align: center middle;
        background: transparent;
    }

    #driver-dialog {
        width: 80;
        height: auto;
        max-height: 90%;
        border: solid $primary;
        background: $surface;
        padding: 1 2;
    }

    #driver-title {
        text-align: center;
        text-style: bold;
        margin-bottom: 1;
    }

    #driver-message {
        margin-bottom: 1;
    }

    #driver-list {
        height: auto;
        max-height: 8;
        background: $surface;
        border: solid $primary-darken-2;
        margin-bottom: 1;
    }

    #install-commands {
        height: auto;
        max-height: 12;
        background: $surface-darken-1;
        padding: 1;
        margin-top: 1;
        overflow-y: auto;
    }

    #driver-footer {
        margin-top: 1;
        text-align: center;
    }
    """

    def __init__(self, installed_drivers: list[str] | None = None):
        super().__init__()
        self.installed_drivers = installed_drivers or []
        self._install_commands: list[str] = []

    def compose(self) -> ComposeResult:
        from .drivers import SUPPORTED_DRIVERS, get_install_commands, get_os_info

        os_type, os_version = get_os_info()
        has_drivers = len(self.installed_drivers) > 0

        with Container(id="driver-dialog"):
            if has_drivers:
                yield Static("Select ODBC Driver", id="driver-title")
                yield Static(
                    f"Found {len(self.installed_drivers)} installed driver(s):",
                    id="driver-message",
                )
            else:
                yield Static("No ODBC Driver Found", id="driver-title")
                yield Static(
                    f"Detected OS: [bold]{os_type}[/] {os_version}\n"
                    "You need an ODBC driver to connect to SQL Server.",
                    id="driver-message",
                )

            # Show installed drivers or available options
            options = []
            if has_drivers:
                for driver in self.installed_drivers:
                    options.append(Option(f"[green]{driver}[/]", id=driver))
            else:
                for driver in SUPPORTED_DRIVERS[:3]:  # Show top 3 options
                    options.append(Option(f"[dim]{driver}[/] (not installed)", id=driver))

            yield OptionList(*options, id="driver-list")

            # Show install commands if no drivers
            if not has_drivers:
                install_info = get_install_commands()
                if install_info:
                    self._install_commands = install_info.commands
                    commands_text = "\n".join(install_info.commands)
                    yield Static(
                        f"[bold]{install_info.description}:[/]\n\n{commands_text}",
                        id="install-commands",
                    )

            footer_text = r"[bold]\[Enter][/] Select"
            if not has_drivers:
                footer_text += r"  [bold]\[I][/] Install"
            footer_text += r"  [bold]\[Esc][/] Cancel"
            yield Static(footer_text, id="driver-footer")

    def on_mount(self) -> None:
        self.query_one("#driver-list", OptionList).focus()

    def action_select(self) -> None:
        option_list = self.query_one("#driver-list", OptionList)
        if option_list.highlighted is not None:
            option = option_list.get_option_at_index(option_list.highlighted)
            self.dismiss(("select", option.id))

    def on_option_list_option_selected(self, event) -> None:
        self.dismiss(("select", event.option.id))

    def action_install_driver(self) -> None:
        """Run installation commands for the selected driver."""
        if not self._install_commands:
            self.notify("No installation commands available", severity="warning")
            return

        from .drivers import get_os_info
        os_type, _ = get_os_info()

        # On Windows, just show instructions
        if os_type == "windows":
            self.notify(
                "Please download and run the installer from Microsoft",
                severity="information",
            )
            return

        self.notify("Installing driver... This may ask for your password.", timeout=5)
        self.dismiss(("install", self._install_commands))

    def action_cancel(self) -> None:
        self.dismiss(None)
