"""Modal screens for sqlit."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal
from textual.screen import ModalScreen
from textual.widgets import DataTable, Input, OptionList, Static
from textual.widgets.option_list import Option

from .config import (
    AUTH_TYPE_LABELS,
    AuthType,
    ConnectionConfig,
    DATABASE_TYPE_LABELS,
    DatabaseType,
)


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

    #dbtype-list {
        height: auto;
        max-height: 6;
        background: $surface;
        border: solid $primary-darken-2;
        padding: 0;
        margin-bottom: 0;
    }

    #dbtype-list > .option-list--option {
        padding: 0 1;
    }

    #server-fields {
        height: auto;
    }

    #server-fields.hidden {
        display: none;
    }

    #mssql-auth-fields {
        height: auto;
    }

    #mssql-auth-fields.hidden {
        display: none;
    }

    #sqlite-fields {
        height: auto;
    }

    #sqlite-fields.hidden {
        display: none;
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

    def _get_initial_db_type(self) -> DatabaseType:
        """Get the initial database type from config."""
        if self.config:
            return self.config.get_db_type()
        return DatabaseType.MSSQL

    def _get_initial_auth_type(self) -> AuthType:
        """Get the initial auth type from config."""
        if self.config:
            return self.config.get_auth_type()
        return AuthType.SQL_SERVER

    def compose(self) -> ComposeResult:
        title = "Edit Connection" if self.editing else "New Connection"
        db_type = self._get_initial_db_type()
        auth_type = self._get_initial_auth_type()
        show_username = auth_type in self.AUTH_NEEDS_USERNAME
        show_password = auth_type in self.AUTH_NEEDS_PASSWORD
        is_mssql = db_type == DatabaseType.MSSQL
        is_sqlite = db_type == DatabaseType.SQLITE
        is_postgresql = db_type == DatabaseType.POSTGRESQL
        is_mysql = db_type == DatabaseType.MYSQL
        is_server_based = db_type in (DatabaseType.MSSQL, DatabaseType.POSTGRESQL, DatabaseType.MYSQL)

        with Container(id="connection-dialog"):
            yield Static(title, id="connection-title")

            yield Static("Name", classes="field-label")
            yield Input(
                value=self.config.name if self.config else "",
                placeholder="My Connection",
                id="conn-name",
            )

            yield Static("Database Type", classes="field-label")
            dbtype_list = OptionList(
                Option(DATABASE_TYPE_LABELS[DatabaseType.MSSQL], id=DatabaseType.MSSQL.value),
                Option(DATABASE_TYPE_LABELS[DatabaseType.POSTGRESQL], id=DatabaseType.POSTGRESQL.value),
                Option(DATABASE_TYPE_LABELS[DatabaseType.MYSQL], id=DatabaseType.MYSQL.value),
                Option(DATABASE_TYPE_LABELS[DatabaseType.SQLITE], id=DatabaseType.SQLITE.value),
                id="dbtype-list",
            )
            yield dbtype_list

            # Server-based database fields (SQL Server, PostgreSQL, MySQL)
            with Container(id="server-fields", classes="" if is_server_based else "hidden"):
                # Get default port based on database type
                default_port = "1433"
                if is_postgresql:
                    default_port = "5432"
                elif is_mysql:
                    default_port = "3306"

                with Horizontal(id="server-port-row"):
                    with Container(id="server-field"):
                        yield Static("Server", classes="field-label")
                        yield Input(
                            value=self.config.server if self.config else "",
                            placeholder="localhost or server\\instance" if is_mssql else "localhost",
                            id="conn-server",
                        )
                    with Container(id="port-field"):
                        yield Static("Port", classes="field-label")
                        yield Input(
                            value=self.config.port if self.config else default_port,
                            placeholder=default_port,
                            id="conn-port",
                        )

                yield Static("Database (empty = browse all)", classes="field-label")
                yield Input(
                    value=self.config.database if self.config else "",
                    placeholder="Leave empty to browse all databases",
                    id="conn-database",
                )

            # SQL Server specific auth fields
            with Container(id="mssql-auth-fields", classes="" if is_mssql else "hidden"):
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

            # Credentials fields - shown for SQL Server (conditional) and always for PostgreSQL/MySQL
            show_credentials = is_postgresql or is_mysql or (is_mssql and show_username)
            with Container(
                id="username-field", classes="" if show_credentials else "hidden"
            ):
                yield Static("Username", classes="field-label")
                yield Input(
                    value=self.config.username if self.config else "",
                    placeholder="user@domain.com" if is_mssql else "username",
                    id="conn-username",
                )

            show_password_field = is_postgresql or is_mysql or (is_mssql and show_password)
            with Container(
                id="password-field", classes="" if show_password_field else "hidden"
            ):
                yield Static("Password", classes="field-label")
                yield Input(
                    value=self.config.password if self.config else "",
                    id="conn-password",
                )

            # SQLite specific fields
            with Container(id="sqlite-fields", classes="" if is_sqlite else "hidden"):
                yield Static("Database File", classes="field-label")
                yield Input(
                    value=self.config.file_path if self.config else "",
                    placeholder="/path/to/database.db",
                    id="conn-filepath",
                )

    def on_mount(self) -> None:
        self.query_one("#conn-name", Input).focus()

        # Set initial database type selection
        dbtype_list = self.query_one("#dbtype-list", OptionList)
        db_type = self._get_initial_db_type()
        dbtype_options = [DatabaseType.MSSQL, DatabaseType.POSTGRESQL, DatabaseType.MYSQL, DatabaseType.SQLITE]
        try:
            dbtype_list.highlighted = dbtype_options.index(db_type)
        except ValueError:
            dbtype_list.highlighted = 0

        # Set initial driver selection (SQL Server only)
        driver_list = self.query_one("#driver-list", OptionList)
        current_driver = self.config.driver if self.config else "ODBC Driver 18 for SQL Server"
        for i in range(driver_list.option_count):
            option = driver_list.get_option_at_index(i)
            if option.id == current_driver:
                driver_list.highlighted = i
                break

        # Set initial auth type selection (SQL Server only)
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
        if event.option_list.id == "dbtype-list":
            try:
                db_type = DatabaseType(event.option.id)
            except ValueError:
                return

            server_fields = self.query_one("#server-fields")
            mssql_auth_fields = self.query_one("#mssql-auth-fields")
            sqlite_fields = self.query_one("#sqlite-fields")
            username_field = self.query_one("#username-field")
            password_field = self.query_one("#password-field")

            is_server_based = db_type in (DatabaseType.MSSQL, DatabaseType.POSTGRESQL, DatabaseType.MYSQL)
            is_sqlite = db_type == DatabaseType.SQLITE
            is_mssql = db_type == DatabaseType.MSSQL

            # Show/hide server fields
            if is_server_based:
                server_fields.remove_class("hidden")
            else:
                server_fields.add_class("hidden")

            # Show/hide SQL Server specific auth fields
            if is_mssql:
                mssql_auth_fields.remove_class("hidden")
            else:
                mssql_auth_fields.add_class("hidden")

            # Show/hide SQLite fields
            if is_sqlite:
                sqlite_fields.remove_class("hidden")
            else:
                sqlite_fields.add_class("hidden")

            # Show/hide credentials - always for PostgreSQL/MySQL, conditional for SQL Server
            if db_type in (DatabaseType.POSTGRESQL, DatabaseType.MYSQL):
                username_field.remove_class("hidden")
                password_field.remove_class("hidden")
            elif is_mssql:
                # For SQL Server, visibility depends on auth type
                auth_list = self.query_one("#auth-list", OptionList)
                auth_idx = auth_list.highlighted or 0
                auth_options = [
                    AuthType.SQL_SERVER,
                    AuthType.WINDOWS,
                    AuthType.AD_PASSWORD,
                    AuthType.AD_INTERACTIVE,
                    AuthType.AD_INTEGRATED,
                ]
                auth_type = auth_options[auth_idx] if auth_idx < len(auth_options) else AuthType.SQL_SERVER
                if auth_type in self.AUTH_NEEDS_USERNAME:
                    username_field.remove_class("hidden")
                else:
                    username_field.add_class("hidden")
                if auth_type in self.AUTH_NEEDS_PASSWORD:
                    password_field.remove_class("hidden")
                else:
                    password_field.add_class("hidden")
            else:
                # SQLite doesn't need credentials
                username_field.add_class("hidden")
                password_field.add_class("hidden")

        elif event.option_list.id == "auth-list":
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

    def _get_selected_db_type(self) -> DatabaseType:
        """Get the currently selected database type."""
        dbtype_list = self.query_one("#dbtype-list", OptionList)
        dbtype_options = [DatabaseType.MSSQL, DatabaseType.POSTGRESQL, DatabaseType.MYSQL, DatabaseType.SQLITE]
        idx = dbtype_list.highlighted or 0
        return dbtype_options[idx] if idx < len(dbtype_options) else DatabaseType.MSSQL

    def _get_focusable_fields(self) -> list:
        """Get list of focusable fields in order."""
        db_type = self._get_selected_db_type()

        fields = [
            self.query_one("#conn-name", Input),
            self.query_one("#dbtype-list", OptionList),
        ]

        if db_type == DatabaseType.MSSQL:
            fields.extend([
                self.query_one("#conn-server", Input),
                self.query_one("#conn-port", Input),
                self.query_one("#conn-database", Input),
                self.query_one("#driver-list", OptionList),
                self.query_one("#auth-list", OptionList),
            ])
            username_field = self.query_one("#username-field")
            password_field = self.query_one("#password-field")
            if "hidden" not in username_field.classes:
                fields.append(self.query_one("#conn-username", Input))
            if "hidden" not in password_field.classes:
                fields.append(self.query_one("#conn-password", Input))
        elif db_type in (DatabaseType.POSTGRESQL, DatabaseType.MYSQL):
            fields.extend([
                self.query_one("#conn-server", Input),
                self.query_one("#conn-port", Input),
                self.query_one("#conn-database", Input),
                self.query_one("#conn-username", Input),
                self.query_one("#conn-password", Input),
            ])
        else:  # SQLite
            fields.append(self.query_one("#conn-filepath", Input))

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
        db_type = self._get_selected_db_type()

        if not name:
            self.notify("Name is required", severity="error")
            return None

        if db_type == DatabaseType.MSSQL:
            server = self.query_one("#conn-server", Input).value
            port = self.query_one("#conn-port", Input).value or "1433"
            database = self.query_one("#conn-database", Input).value
            driver = self._get_selected_driver()
            auth_type = self._get_selected_auth_type()

            username = ""
            password = ""

            if auth_type in self.AUTH_NEEDS_USERNAME:
                username = self.query_one("#conn-username", Input).value
            if auth_type in self.AUTH_NEEDS_PASSWORD:
                password = self.query_one("#conn-password", Input).value

            if not server:
                self.notify("Server is required", severity="error")
                return None

            if auth_type in self.AUTH_NEEDS_USERNAME and not username:
                self.notify(
                    "Username is required for this authentication type", severity="error"
                )
                return None

            return ConnectionConfig(
                name=name,
                db_type=db_type.value,
                server=server,
                port=port,
                database=database,
                username=username,
                password=password,
                auth_type=auth_type.value,
                driver=driver,
                trusted_connection=(auth_type == AuthType.WINDOWS),
            )
        elif db_type in (DatabaseType.POSTGRESQL, DatabaseType.MYSQL):
            server = self.query_one("#conn-server", Input).value
            default_port = "5432" if db_type == DatabaseType.POSTGRESQL else "3306"
            port = self.query_one("#conn-port", Input).value or default_port
            database = self.query_one("#conn-database", Input).value
            username = self.query_one("#conn-username", Input).value
            password = self.query_one("#conn-password", Input).value

            if not server:
                self.notify("Server is required", severity="error")
                return None

            if not username:
                self.notify("Username is required", severity="error")
                return None

            return ConnectionConfig(
                name=name,
                db_type=db_type.value,
                server=server,
                port=port,
                database=database,
                username=username,
                password=password,
            )
        else:  # SQLite
            file_path = self.query_one("#conn-filepath", Input).value

            if not file_path:
                self.notify("Database file path is required", severity="error")
                return None

            return ConnectionConfig(
                name=name,
                db_type=db_type.value,
                file_path=file_path,
            )

    def _get_package_install_hint(self, db_type: str) -> str | None:
        """Get pip install command for missing database packages."""
        hints = {
            "postgresql": "pip install psycopg2-binary",
            "mysql": "pip install mysql-connector-python",
        }
        return hints.get(db_type)

    def action_test_connection(self) -> None:
        """Test the connection without saving or closing."""
        config = self._get_config()
        if not config:
            return

        try:
            from .adapters import get_adapter

            adapter = get_adapter(config.db_type)
            conn = adapter.connect(config)
            conn.close()
            self.notify("Connection successful!", severity="information")
        except ModuleNotFoundError as e:
            hint = self._get_package_install_hint(config.db_type)
            if hint:
                self.notify(f"Missing package. Install with: {hint}", severity="error")
            else:
                self.notify(f"Required module not installed: {e}", severity="error")
        except ImportError as e:
            hint = self._get_package_install_hint(config.db_type)
            if hint:
                self.notify(f"Missing package. Install with: {hint}", severity="error")
            else:
                self.notify(f"Required module not installed: {e}", severity="error")
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


class QueryHistoryScreen(ModalScreen):
    """Modal screen for query history selection."""

    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("enter", "select", "Select"),
        Binding("d", "delete", "Delete"),
    ]

    CSS = """
    QueryHistoryScreen {
        align: center middle;
        background: transparent;
    }

    #history-dialog {
        width: 90;
        height: 80%;
        border: solid $primary;
        background: $surface;
        padding: 1 2;
    }

    #history-title {
        text-align: center;
        text-style: bold;
        margin-bottom: 1;
    }

    #history-list {
        height: 1fr;
        background: $surface;
        border: solid $primary-darken-2;
        padding: 0;
    }

    #history-list > .option-list--option {
        padding: 0 1;
    }

    #history-preview {
        height: 8;
        background: $surface-darken-1;
        border: solid $primary-darken-2;
        padding: 1;
        margin-top: 1;
        overflow-y: auto;
    }

    #history-footer {
        margin-top: 1;
        text-align: center;
        color: $text-muted;
    }
    """

    def __init__(self, history: list, connection_name: str):
        super().__init__()
        self.history = history  # list of QueryHistoryEntry
        self.connection_name = connection_name

    def compose(self) -> ComposeResult:
        from datetime import datetime

        with Container(id="history-dialog"):
            yield Static(f"Query History - {self.connection_name}", id="history-title")

            options = []
            for entry in self.history:
                # Format timestamp nicely
                try:
                    dt = datetime.fromisoformat(entry.timestamp)
                    time_str = dt.strftime("%Y-%m-%d %H:%M")
                except (ValueError, AttributeError):
                    time_str = "Unknown"

                # Truncate query for display
                query_preview = entry.query.replace("\n", " ")[:50]
                if len(entry.query) > 50:
                    query_preview += "..."

                options.append(Option(f"[dim]{time_str}[/] {query_preview}", id=entry.timestamp))

            if options:
                yield OptionList(*options, id="history-list")
            else:
                yield Static("No query history for this connection", id="history-list")

            yield Static("", id="history-preview")
            yield Static(r"[bold]\[Enter][/] Select  [bold]\[D][/] Delete  [bold]\[Esc][/] Cancel", id="history-footer")

    def on_mount(self) -> None:
        try:
            option_list = self.query_one("#history-list", OptionList)
            option_list.focus()
            if self.history:
                self._update_preview(0)
        except Exception:
            pass

    def on_option_list_option_highlighted(self, event) -> None:
        if event.option_list.id == "history-list":
            idx = event.option_list.highlighted
            if idx is not None:
                self._update_preview(idx)

    def _update_preview(self, idx: int) -> None:
        if idx < len(self.history):
            preview = self.query_one("#history-preview", Static)
            preview.update(self.history[idx].query)

    def action_select(self) -> None:
        if not self.history:
            self.dismiss(None)
            return

        try:
            option_list = self.query_one("#history-list", OptionList)
            idx = option_list.highlighted
            if idx is not None and idx < len(self.history):
                self.dismiss(("select", self.history[idx].query))
            else:
                self.dismiss(None)
        except Exception:
            self.dismiss(None)

    def on_option_list_option_selected(self, event) -> None:
        if event.option_list.id == "history-list":
            idx = event.option_list.highlighted
            if idx is not None and idx < len(self.history):
                self.dismiss(("select", self.history[idx].query))

    def action_delete(self) -> None:
        """Delete the selected history entry."""
        if not self.history:
            return

        try:
            option_list = self.query_one("#history-list", OptionList)
            idx = option_list.highlighted
            if idx is not None and idx < len(self.history):
                # Remove from history and refresh
                entry = self.history[idx]
                self.dismiss(("delete", entry.timestamp))
        except Exception:
            pass

    def action_cancel(self) -> None:
        self.dismiss(None)
