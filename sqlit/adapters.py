"""Database adapters for sqlit - abstraction layer for different database types."""

from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .config import ConnectionConfig


@dataclass
class ColumnInfo:
    """Information about a database column."""

    name: str
    data_type: str


class DatabaseAdapter(ABC):
    """Abstract base class for database adapters."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable name for this database type."""
        pass

    @property
    @abstractmethod
    def supports_multiple_databases(self) -> bool:
        """Whether this database type supports multiple databases."""
        pass

    @property
    @abstractmethod
    def supports_stored_procedures(self) -> bool:
        """Whether this database type supports stored procedures."""
        pass

    @abstractmethod
    def connect(self, config: "ConnectionConfig") -> Any:
        """Create a connection to the database."""
        pass

    @abstractmethod
    def get_databases(self, conn: Any) -> list[str]:
        """Get list of databases (if supported)."""
        pass

    @abstractmethod
    def get_tables(self, conn: Any, database: str | None = None) -> list[str]:
        """Get list of tables in the database."""
        pass

    @abstractmethod
    def get_views(self, conn: Any, database: str | None = None) -> list[str]:
        """Get list of views in the database."""
        pass

    @abstractmethod
    def get_columns(
        self, conn: Any, table: str, database: str | None = None
    ) -> list[ColumnInfo]:
        """Get list of columns for a table."""
        pass

    @abstractmethod
    def get_procedures(self, conn: Any, database: str | None = None) -> list[str]:
        """Get list of stored procedures (if supported)."""
        pass

    @abstractmethod
    def quote_identifier(self, name: str) -> str:
        """Quote an identifier (table name, column name, etc.)."""
        pass

    @abstractmethod
    def build_select_query(
        self, table: str, limit: int, database: str | None = None
    ) -> str:
        """Build a SELECT query with limit."""
        pass

    @abstractmethod
    def execute_query(self, conn: Any, query: str) -> tuple[list[str], list[tuple]]:
        """Execute a query and return (columns, rows)."""
        pass

    @abstractmethod
    def execute_non_query(self, conn: Any, query: str) -> int:
        """Execute a non-query statement and return rows affected."""
        pass


class SQLServerAdapter(DatabaseAdapter):
    """Adapter for Microsoft SQL Server using pyodbc."""

    @property
    def name(self) -> str:
        return "SQL Server"

    @property
    def supports_multiple_databases(self) -> bool:
        return True

    @property
    def supports_stored_procedures(self) -> bool:
        return True

    def connect(self, config: "ConnectionConfig") -> Any:
        """Connect to SQL Server using pyodbc."""
        import pyodbc

        conn_str = config.get_connection_string()
        return pyodbc.connect(conn_str, timeout=10)

    def get_databases(self, conn: Any) -> list[str]:
        """Get list of databases from SQL Server."""
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sys.databases ORDER BY name")
        return [row[0] for row in cursor.fetchall()]

    def get_tables(self, conn: Any, database: str | None = None) -> list[str]:
        """Get list of tables from SQL Server."""
        cursor = conn.cursor()
        if database:
            cursor.execute(
                f"SELECT TABLE_NAME FROM [{database}].INFORMATION_SCHEMA.TABLES "
                f"WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME"
            )
        else:
            cursor.execute(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME"
            )
        return [row[0] for row in cursor.fetchall()]

    def get_views(self, conn: Any, database: str | None = None) -> list[str]:
        """Get list of views from SQL Server."""
        cursor = conn.cursor()
        if database:
            cursor.execute(
                f"SELECT TABLE_NAME FROM [{database}].INFORMATION_SCHEMA.VIEWS "
                f"ORDER BY TABLE_NAME"
            )
        else:
            cursor.execute(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS ORDER BY TABLE_NAME"
            )
        return [row[0] for row in cursor.fetchall()]

    def get_columns(
        self, conn: Any, table: str, database: str | None = None
    ) -> list[ColumnInfo]:
        """Get columns for a table from SQL Server."""
        cursor = conn.cursor()
        if database:
            cursor.execute(
                f"SELECT COLUMN_NAME, DATA_TYPE FROM [{database}].INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
                (table,),
            )
        else:
            cursor.execute(
                "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
                (table,),
            )
        return [ColumnInfo(name=row[0], data_type=row[1]) for row in cursor.fetchall()]

    def get_procedures(self, conn: Any, database: str | None = None) -> list[str]:
        """Get stored procedures from SQL Server."""
        cursor = conn.cursor()
        if database:
            cursor.execute(
                f"SELECT ROUTINE_NAME FROM [{database}].INFORMATION_SCHEMA.ROUTINES "
                f"WHERE ROUTINE_TYPE = 'PROCEDURE' ORDER BY ROUTINE_NAME"
            )
        else:
            cursor.execute(
                "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES "
                "WHERE ROUTINE_TYPE = 'PROCEDURE' ORDER BY ROUTINE_NAME"
            )
        return [row[0] for row in cursor.fetchall()]

    def quote_identifier(self, name: str) -> str:
        """Quote identifier using SQL Server brackets."""
        return f"[{name}]"

    def build_select_query(
        self, table: str, limit: int, database: str | None = None
    ) -> str:
        """Build SELECT TOP query for SQL Server."""
        if database:
            return f"SELECT TOP {limit} * FROM [{database}].[dbo].[{table}]"
        return f"SELECT TOP {limit} * FROM [{table}]"

    def execute_query(self, conn: Any, query: str) -> tuple[list[str], list[tuple]]:
        """Execute a query on SQL Server."""
        cursor = conn.cursor()
        cursor.execute(query)
        if cursor.description:
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return columns, [tuple(row) for row in rows]
        return [], []

    def execute_non_query(self, conn: Any, query: str) -> int:
        """Execute a non-query on SQL Server."""
        cursor = conn.cursor()
        cursor.execute(query)
        rowcount = cursor.rowcount
        conn.commit()
        return rowcount


class PostgreSQLAdapter(DatabaseAdapter):
    """Adapter for PostgreSQL using psycopg2."""

    @property
    def name(self) -> str:
        return "PostgreSQL"

    @property
    def supports_multiple_databases(self) -> bool:
        return True

    @property
    def supports_stored_procedures(self) -> bool:
        return True

    def connect(self, config: "ConnectionConfig") -> Any:
        """Connect to PostgreSQL database."""
        import psycopg2

        port = int(config.port) if config.port else 5432
        conn = psycopg2.connect(
            host=config.server,
            port=port,
            database=config.database or "postgres",
            user=config.username,
            password=config.password,
            connect_timeout=10,
        )
        # Enable autocommit to avoid "transaction aborted" errors on failed statements
        conn.autocommit = True
        return conn

    def get_databases(self, conn: Any) -> list[str]:
        """Get list of databases from PostgreSQL."""
        cursor = conn.cursor()
        cursor.execute(
            "SELECT datname FROM pg_database "
            "WHERE datistemplate = false ORDER BY datname"
        )
        return [row[0] for row in cursor.fetchall()]

    def get_tables(self, conn: Any, database: str | None = None) -> list[str]:
        """Get list of tables from PostgreSQL."""
        cursor = conn.cursor()
        cursor.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' "
            "ORDER BY table_name"
        )
        return [row[0] for row in cursor.fetchall()]

    def get_views(self, conn: Any, database: str | None = None) -> list[str]:
        """Get list of views from PostgreSQL."""
        cursor = conn.cursor()
        cursor.execute(
            "SELECT table_name FROM information_schema.views "
            "WHERE table_schema = 'public' ORDER BY table_name"
        )
        return [row[0] for row in cursor.fetchall()]

    def get_columns(
        self, conn: Any, table: str, database: str | None = None
    ) -> list[ColumnInfo]:
        """Get columns for a table from PostgreSQL."""
        cursor = conn.cursor()
        cursor.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s "
            "ORDER BY ordinal_position",
            (table,),
        )
        return [ColumnInfo(name=row[0], data_type=row[1]) for row in cursor.fetchall()]

    def get_procedures(self, conn: Any, database: str | None = None) -> list[str]:
        """Get stored procedures/functions from PostgreSQL."""
        cursor = conn.cursor()
        cursor.execute(
            "SELECT routine_name FROM information_schema.routines "
            "WHERE routine_schema = 'public' AND routine_type = 'FUNCTION' "
            "ORDER BY routine_name"
        )
        return [row[0] for row in cursor.fetchall()]

    def quote_identifier(self, name: str) -> str:
        """Quote identifier using double quotes for PostgreSQL."""
        return f'"{name}"'

    def build_select_query(
        self, table: str, limit: int, database: str | None = None
    ) -> str:
        """Build SELECT LIMIT query for PostgreSQL."""
        return f'SELECT * FROM "{table}" LIMIT {limit}'

    def execute_query(self, conn: Any, query: str) -> tuple[list[str], list[tuple]]:
        """Execute a query on PostgreSQL."""
        cursor = conn.cursor()
        cursor.execute(query)
        if cursor.description:
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return columns, [tuple(row) for row in rows]
        return [], []

    def execute_non_query(self, conn: Any, query: str) -> int:
        """Execute a non-query on PostgreSQL."""
        cursor = conn.cursor()
        cursor.execute(query)
        rowcount = cursor.rowcount
        conn.commit()
        return rowcount


class MySQLAdapter(DatabaseAdapter):
    """Adapter for MySQL using mysql-connector-python."""

    @property
    def name(self) -> str:
        return "MySQL"

    @property
    def supports_multiple_databases(self) -> bool:
        return True

    @property
    def supports_stored_procedures(self) -> bool:
        return True

    def connect(self, config: "ConnectionConfig") -> Any:
        """Connect to MySQL database."""
        import mysql.connector

        port = int(config.port) if config.port else 3306
        return mysql.connector.connect(
            host=config.server,
            port=port,
            database=config.database or None,
            user=config.username,
            password=config.password,
            connection_timeout=10,
        )

    def get_databases(self, conn: Any) -> list[str]:
        """Get list of databases from MySQL."""
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        return [row[0] for row in cursor.fetchall()]

    def get_tables(self, conn: Any, database: str | None = None) -> list[str]:
        """Get list of tables from MySQL."""
        cursor = conn.cursor()
        if database:
            cursor.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
                "ORDER BY table_name",
                (database,),
            )
        else:
            cursor.execute("SHOW TABLES")
        return [row[0] for row in cursor.fetchall()]

    def get_views(self, conn: Any, database: str | None = None) -> list[str]:
        """Get list of views from MySQL."""
        cursor = conn.cursor()
        if database:
            cursor.execute(
                "SELECT table_name FROM information_schema.views "
                "WHERE table_schema = %s ORDER BY table_name",
                (database,),
            )
        else:
            cursor.execute(
                "SELECT table_name FROM information_schema.views "
                "WHERE table_schema = DATABASE() ORDER BY table_name"
            )
        return [row[0] for row in cursor.fetchall()]

    def get_columns(
        self, conn: Any, table: str, database: str | None = None
    ) -> list[ColumnInfo]:
        """Get columns for a table from MySQL."""
        cursor = conn.cursor()
        if database:
            cursor.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s "
                "ORDER BY ordinal_position",
                (database, table),
            )
        else:
            cursor.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = DATABASE() AND table_name = %s "
                "ORDER BY ordinal_position",
                (table,),
            )
        return [ColumnInfo(name=row[0], data_type=row[1]) for row in cursor.fetchall()]

    def get_procedures(self, conn: Any, database: str | None = None) -> list[str]:
        """Get stored procedures from MySQL."""
        cursor = conn.cursor()
        if database:
            cursor.execute(
                "SELECT routine_name FROM information_schema.routines "
                "WHERE routine_schema = %s AND routine_type = 'PROCEDURE' "
                "ORDER BY routine_name",
                (database,),
            )
        else:
            cursor.execute(
                "SELECT routine_name FROM information_schema.routines "
                "WHERE routine_schema = DATABASE() AND routine_type = 'PROCEDURE' "
                "ORDER BY routine_name"
            )
        return [row[0] for row in cursor.fetchall()]

    def quote_identifier(self, name: str) -> str:
        """Quote identifier using backticks for MySQL."""
        return f"`{name}`"

    def build_select_query(
        self, table: str, limit: int, database: str | None = None
    ) -> str:
        """Build SELECT LIMIT query for MySQL."""
        if database:
            return f"SELECT * FROM `{database}`.`{table}` LIMIT {limit}"
        return f"SELECT * FROM `{table}` LIMIT {limit}"

    def execute_query(self, conn: Any, query: str) -> tuple[list[str], list[tuple]]:
        """Execute a query on MySQL."""
        cursor = conn.cursor()
        cursor.execute(query)
        if cursor.description:
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return columns, [tuple(row) for row in rows]
        return [], []

    def execute_non_query(self, conn: Any, query: str) -> int:
        """Execute a non-query on MySQL."""
        cursor = conn.cursor()
        cursor.execute(query)
        rowcount = cursor.rowcount
        conn.commit()
        return rowcount


class SQLiteAdapter(DatabaseAdapter):
    """Adapter for SQLite using built-in sqlite3."""

    @property
    def name(self) -> str:
        return "SQLite"

    @property
    def supports_multiple_databases(self) -> bool:
        return False

    @property
    def supports_stored_procedures(self) -> bool:
        return False

    def connect(self, config: "ConnectionConfig") -> Any:
        """Connect to SQLite database file."""
        from pathlib import Path

        path_str = config.file_path.strip()

        # Expand ~ to home directory
        file_path = Path(path_str).expanduser()

        # If path doesn't exist and looks like a missing leading slash, try adding it
        if not file_path.exists() and not path_str.startswith(("/", "~")):
            absolute_path = Path("/" + path_str)
            if absolute_path.exists():
                file_path = absolute_path

        # Resolve to absolute path
        file_path = file_path.resolve()

        conn = sqlite3.connect(file_path)
        conn.row_factory = sqlite3.Row
        return conn

    def get_databases(self, conn: Any) -> list[str]:
        """SQLite doesn't support multiple databases - return empty list."""
        return []

    def get_tables(self, conn: Any, database: str | None = None) -> list[str]:
        """Get list of tables from SQLite."""
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        return [row[0] for row in cursor.fetchall()]

    def get_views(self, conn: Any, database: str | None = None) -> list[str]:
        """Get list of views from SQLite."""
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"
        )
        return [row[0] for row in cursor.fetchall()]

    def get_columns(
        self, conn: Any, table: str, database: str | None = None
    ) -> list[ColumnInfo]:
        """Get columns for a table from SQLite."""
        cursor = conn.cursor()
        cursor.execute(f'PRAGMA table_info("{table}")')
        # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
        return [ColumnInfo(name=row[1], data_type=row[2] or "TEXT") for row in cursor.fetchall()]

    def get_procedures(self, conn: Any, database: str | None = None) -> list[str]:
        """SQLite doesn't support stored procedures - return empty list."""
        return []

    def quote_identifier(self, name: str) -> str:
        """Quote identifier using double quotes for SQLite."""
        return f'"{name}"'

    def build_select_query(
        self, table: str, limit: int, database: str | None = None
    ) -> str:
        """Build SELECT LIMIT query for SQLite."""
        return f'SELECT * FROM "{table}" LIMIT {limit}'

    def execute_query(self, conn: Any, query: str) -> tuple[list[str], list[tuple]]:
        """Execute a query on SQLite."""
        cursor = conn.cursor()
        cursor.execute(query)
        if cursor.description:
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return columns, [tuple(row) for row in rows]
        return [], []

    def execute_non_query(self, conn: Any, query: str) -> int:
        """Execute a non-query on SQLite."""
        cursor = conn.cursor()
        cursor.execute(query)
        rowcount = cursor.rowcount
        conn.commit()
        return rowcount


def get_adapter(db_type: str) -> DatabaseAdapter:
    """Get the appropriate adapter for a database type."""
    adapters = {
        "mssql": SQLServerAdapter(),
        "sqlite": SQLiteAdapter(),
        "postgresql": PostgreSQLAdapter(),
        "mysql": MySQLAdapter(),
    }
    adapter = adapters.get(db_type)
    if not adapter:
        raise ValueError(f"Unknown database type: {db_type}")
    return adapter
