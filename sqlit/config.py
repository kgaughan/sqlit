"""Configuration management for sqlit."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path


class DatabaseType(Enum):
    """Supported database types."""

    MSSQL = "mssql"
    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"


DATABASE_TYPE_LABELS = {
    DatabaseType.MSSQL: "SQL Server",
    DatabaseType.SQLITE: "SQLite",
    DatabaseType.POSTGRESQL: "PostgreSQL",
    DatabaseType.MYSQL: "MySQL",
}


class AuthType(Enum):
    """Authentication types for SQL Server connections."""

    WINDOWS = "windows"
    SQL_SERVER = "sql"
    AD_PASSWORD = "ad_password"
    AD_INTERACTIVE = "ad_interactive"
    AD_INTEGRATED = "ad_integrated"


AUTH_TYPE_LABELS = {
    AuthType.WINDOWS: "Windows Authentication",
    AuthType.SQL_SERVER: "SQL Server Authentication",
    AuthType.AD_PASSWORD: "Microsoft Entra Password",
    AuthType.AD_INTERACTIVE: "Microsoft Entra MFA",
    AuthType.AD_INTEGRATED: "Microsoft Entra Integrated",
}


@dataclass
class ConnectionConfig:
    """Database connection configuration."""

    name: str
    db_type: str = "mssql"  # Database type: mssql, sqlite, postgresql, mysql
    # Server-based database fields (SQL Server, PostgreSQL, MySQL)
    server: str = ""
    port: str = "1433"  # Default varies: 1433 (MSSQL), 5432 (PostgreSQL), 3306 (MySQL)
    database: str = ""
    username: str = ""
    password: str = ""
    # SQL Server specific fields
    auth_type: str = "sql"
    driver: str = "ODBC Driver 18 for SQL Server"
    trusted_connection: bool = False  # Legacy field for backwards compatibility
    # SQLite specific fields
    file_path: str = ""

    def __post_init__(self):
        """Handle backwards compatibility with old configs."""
        # Old configs without db_type are SQL Server
        if not hasattr(self, "db_type") or not self.db_type:
            self.db_type = "mssql"
        # Handle old SQL Server auth compatibility
        if self.db_type == "mssql":
            if self.auth_type == "windows" and not self.trusted_connection and self.username:
                self.auth_type = "sql"

    def get_db_type(self) -> DatabaseType:
        """Get the DatabaseType enum value."""
        try:
            return DatabaseType(self.db_type)
        except ValueError:
            return DatabaseType.MSSQL

    def get_auth_type(self) -> AuthType:
        """Get the AuthType enum value."""
        try:
            return AuthType(self.auth_type)
        except ValueError:
            return AuthType.SQL_SERVER

    def get_connection_string(self) -> str:
        """Build the connection string for SQL Server."""
        if self.db_type != "mssql":
            raise ValueError("get_connection_string() is only for SQL Server connections")

        server_with_port = self.server
        if self.port and self.port != "1433":
            server_with_port = f"{self.server},{self.port}"

        base = (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={server_with_port};"
            f"DATABASE={self.database or 'master'};"
            f"TrustServerCertificate=yes;"
        )

        auth = self.get_auth_type()

        if auth == AuthType.WINDOWS:
            return base + "Trusted_Connection=yes;"
        elif auth == AuthType.SQL_SERVER:
            return base + f"UID={self.username};PWD={self.password};"
        elif auth == AuthType.AD_PASSWORD:
            return (
                base
                + f"Authentication=ActiveDirectoryPassword;"
                f"UID={self.username};PWD={self.password};"
            )
        elif auth == AuthType.AD_INTERACTIVE:
            return (
                base + f"Authentication=ActiveDirectoryInteractive;" f"UID={self.username};"
            )
        elif auth == AuthType.AD_INTEGRATED:
            return base + "Authentication=ActiveDirectoryIntegrated;"

        return base + "Trusted_Connection=yes;"

    def get_display_info(self) -> str:
        """Get a display string for the connection."""
        if self.db_type == "sqlite":
            return self.name
        else:
            # Server-based databases (mssql, postgresql, mysql)
            db_part = f"@{self.database}" if self.database else ""
            return f"{self.name}{db_part}"


CONFIG_DIR = Path.home() / ".sqlit"
CONFIG_PATH = CONFIG_DIR / "connections.json"
SETTINGS_PATH = CONFIG_DIR / "settings.json"
HISTORY_PATH = CONFIG_DIR / "query_history.json"


def load_settings() -> dict:
    """Load app settings from config file."""
    if not SETTINGS_PATH.exists():
        return {}
    try:
        with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, TypeError):
        return {}


def save_settings(settings: dict) -> None:
    """Save app settings to config file."""
    SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(settings, f, indent=2)


def load_connections() -> list[ConnectionConfig]:
    """Load saved connections from config file."""
    if not CONFIG_PATH.exists():
        return []
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            return [ConnectionConfig(**conn) for conn in data]
    except (json.JSONDecodeError, TypeError):
        return []


def save_connections(connections: list[ConnectionConfig]) -> None:
    """Save connections to config file."""
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump([vars(c) for c in connections], f, indent=2)


@dataclass
class QueryHistoryEntry:
    """A query history entry."""

    query: str
    timestamp: str  # ISO format
    connection_name: str

    def to_dict(self) -> dict:
        return {
            "query": self.query,
            "timestamp": self.timestamp,
            "connection_name": self.connection_name,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "QueryHistoryEntry":
        return cls(
            query=data["query"],
            timestamp=data["timestamp"],
            connection_name=data["connection_name"],
        )


def load_query_history(connection_name: str) -> list[QueryHistoryEntry]:
    """Load query history for a specific connection, sorted by most recent first."""
    if not HISTORY_PATH.exists():
        return []
    try:
        with open(HISTORY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            entries = [
                QueryHistoryEntry.from_dict(entry)
                for entry in data
                if entry.get("connection_name") == connection_name
            ]
            # Sort by timestamp descending (most recent first)
            entries.sort(key=lambda e: e.timestamp, reverse=True)
            return entries
    except (json.JSONDecodeError, TypeError, KeyError):
        return []


def save_query_to_history(connection_name: str, query: str) -> None:
    """Save a query to history for a connection."""
    from datetime import datetime

    # Load existing history
    all_entries: list[dict] = []
    if HISTORY_PATH.exists():
        try:
            with open(HISTORY_PATH, "r", encoding="utf-8") as f:
                all_entries = json.load(f)
        except (json.JSONDecodeError, TypeError):
            all_entries = []

    # Check if this exact query already exists for this connection
    query_stripped = query.strip()
    for entry in all_entries:
        if entry.get("connection_name") == connection_name and entry.get("query", "").strip() == query_stripped:
            # Update timestamp of existing entry
            entry["timestamp"] = datetime.now().isoformat()
            break
    else:
        # Add new entry
        new_entry = QueryHistoryEntry(
            query=query_stripped,
            timestamp=datetime.now().isoformat(),
            connection_name=connection_name,
        )
        all_entries.append(new_entry.to_dict())

    # Keep only last 100 entries per connection
    connection_entries = [e for e in all_entries if e.get("connection_name") == connection_name]
    other_entries = [e for e in all_entries if e.get("connection_name") != connection_name]

    # Sort and limit connection entries
    connection_entries.sort(key=lambda e: e.get("timestamp", ""), reverse=True)
    connection_entries = connection_entries[:100]

    # Save
    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(other_entries + connection_entries, f, indent=2)
