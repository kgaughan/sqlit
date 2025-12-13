"""Integration tests for sqlit CLI commands."""

from __future__ import annotations

import json

import pytest


# =============================================================================
# SQLite Integration Tests
# =============================================================================


class TestSQLiteIntegration:
    """Integration tests for SQLite database operations via CLI."""

    def test_create_sqlite_connection(self, sqlite_db, cli_runner):
        """Test creating a SQLite connection via CLI."""
        connection_name = "test_create_sqlite"

        try:
            # Create connection
            result = cli_runner(
                "connection", "create",
                "--name", connection_name,
                "--db-type", "sqlite",
                "--file-path", str(sqlite_db),
            )
            assert result.returncode == 0
            assert "created successfully" in result.stdout

            # Verify it appears in list
            result = cli_runner("connection", "list")
            assert connection_name in result.stdout
            assert "SQLite" in result.stdout

        finally:
            # Cleanup
            cli_runner("connection", "delete", connection_name, check=False)

    def test_list_connections_shows_sqlite(self, sqlite_connection, cli_runner):
        """Test that connection list shows SQLite connections correctly."""
        result = cli_runner("connection", "list")
        assert result.returncode == 0
        assert sqlite_connection in result.stdout
        assert "SQLite" in result.stdout

    def test_query_sqlite_select(self, sqlite_connection, cli_runner):
        """Test executing SELECT query on SQLite."""
        result = cli_runner(
            "query",
            "-c", sqlite_connection,
            "-q", "SELECT * FROM test_users ORDER BY id",
        )
        assert result.returncode == 0
        assert "Alice" in result.stdout
        assert "Bob" in result.stdout
        assert "Charlie" in result.stdout
        assert "3 row(s) returned" in result.stdout

    def test_query_sqlite_with_where(self, sqlite_connection, cli_runner):
        """Test executing SELECT with WHERE clause on SQLite."""
        result = cli_runner(
            "query",
            "-c", sqlite_connection,
            "-q", "SELECT name, email FROM test_users WHERE id = 1",
        )
        assert result.returncode == 0
        assert "Alice" in result.stdout
        assert "alice@example.com" in result.stdout
        assert "1 row(s) returned" in result.stdout

    def test_query_sqlite_json_format(self, sqlite_connection, cli_runner):
        """Test query output in JSON format."""
        result = cli_runner(
            "query",
            "-c", sqlite_connection,
            "-q", "SELECT id, name FROM test_users ORDER BY id LIMIT 2",
            "--format", "json",
        )
        assert result.returncode == 0

        # Parse JSON output (exclude the row count message)
        lines = result.stdout.strip().split("\n")
        json_output = "\n".join(lines[:-1])  # Remove "(X row(s) returned)"
        data = json.loads(json_output)

        assert len(data) == 2
        assert data[0]["name"] == "Alice"
        assert data[1]["name"] == "Bob"

    def test_query_sqlite_csv_format(self, sqlite_connection, cli_runner):
        """Test query output in CSV format."""
        result = cli_runner(
            "query",
            "-c", sqlite_connection,
            "-q", "SELECT id, name FROM test_users ORDER BY id LIMIT 2",
            "--format", "csv",
        )
        assert result.returncode == 0
        assert "id,name" in result.stdout
        assert "1,Alice" in result.stdout
        assert "2,Bob" in result.stdout

    def test_query_sqlite_view(self, sqlite_connection, cli_runner):
        """Test querying a view."""
        result = cli_runner(
            "query",
            "-c", sqlite_connection,
            "-q", "SELECT * FROM test_user_emails ORDER BY id",
        )
        assert result.returncode == 0
        assert "Alice" in result.stdout
        assert "3 row(s) returned" in result.stdout

    def test_query_sqlite_aggregate(self, sqlite_connection, cli_runner):
        """Test aggregate query on SQLite."""
        result = cli_runner(
            "query",
            "-c", sqlite_connection,
            "-q", "SELECT COUNT(*) as user_count FROM test_users",
        )
        assert result.returncode == 0
        assert "3" in result.stdout

    def test_query_sqlite_join(self, sqlite_connection, cli_runner):
        """Test JOIN query on SQLite."""
        # This test verifies that complex queries work
        result = cli_runner(
            "query",
            "-c", sqlite_connection,
            "-q", """
                SELECT u.name, p.name as product, p.price
                FROM test_users u
                CROSS JOIN test_products p
                WHERE u.id = 1 AND p.id = 1
            """,
        )
        assert result.returncode == 0
        assert "Alice" in result.stdout
        assert "Widget" in result.stdout

    def test_query_sqlite_insert(self, sqlite_connection, cli_runner):
        """Test INSERT statement on SQLite."""
        result = cli_runner(
            "query",
            "-c", sqlite_connection,
            "-q", "INSERT INTO test_users (id, name, email) VALUES (4, 'David', 'david@example.com')",
        )
        assert result.returncode == 0
        assert "row(s) affected" in result.stdout.lower() or "executed successfully" in result.stdout.lower()

        # Verify the insert
        result = cli_runner(
            "query",
            "-c", sqlite_connection,
            "-q", "SELECT * FROM test_users WHERE id = 4",
        )
        assert "David" in result.stdout

    def test_query_sqlite_update(self, sqlite_connection, cli_runner):
        """Test UPDATE statement on SQLite."""
        result = cli_runner(
            "query",
            "-c", sqlite_connection,
            "-q", "UPDATE test_products SET stock = 200 WHERE id = 1",
        )
        assert result.returncode == 0

        # Verify the update
        result = cli_runner(
            "query",
            "-c", sqlite_connection,
            "-q", "SELECT stock FROM test_products WHERE id = 1",
        )
        assert "200" in result.stdout

    def test_delete_sqlite_connection(self, sqlite_db, cli_runner):
        """Test deleting a SQLite connection."""
        connection_name = "test_delete_sqlite"

        # Create connection first
        cli_runner(
            "connection", "create",
            "--name", connection_name,
            "--db-type", "sqlite",
            "--file-path", str(sqlite_db),
        )

        # Delete it
        result = cli_runner("connection", "delete", connection_name)
        assert result.returncode == 0
        assert "deleted successfully" in result.stdout

        # Verify it's gone
        result = cli_runner("connection", "list")
        assert connection_name not in result.stdout

    def test_query_sqlite_invalid_query(self, sqlite_connection, cli_runner):
        """Test handling of invalid SQL query."""
        result = cli_runner(
            "query",
            "-c", sqlite_connection,
            "-q", "SELECT * FROM nonexistent_table",
            check=False,
        )
        assert result.returncode != 0
        assert "error" in result.stdout.lower() or "error" in result.stderr.lower()


# =============================================================================
# SQL Server Integration Tests
# =============================================================================


class TestMSSQLIntegration:
    """Integration tests for SQL Server database operations via CLI.

    These tests require a running SQL Server instance (via Docker).
    Tests are skipped if SQL Server is not available.
    """

    def test_create_mssql_connection(self, mssql_db, cli_runner):
        """Test creating a SQL Server connection via CLI."""
        from .conftest import MSSQL_HOST, MSSQL_PORT, MSSQL_USER, MSSQL_PASSWORD

        connection_name = "test_create_mssql"

        try:
            # Create connection
            result = cli_runner(
                "connection", "create",
                "--name", connection_name,
                "--db-type", "mssql",
                "--server", f"{MSSQL_HOST},{MSSQL_PORT}" if MSSQL_PORT != 1433 else MSSQL_HOST,
                "--database", mssql_db,
                "--auth-type", "sql",
                "--username", MSSQL_USER,
                "--password", MSSQL_PASSWORD,
            )
            assert result.returncode == 0
            assert "created successfully" in result.stdout

            # Verify it appears in list
            result = cli_runner("connection", "list")
            assert connection_name in result.stdout
            assert "SQL Server" in result.stdout

        finally:
            # Cleanup
            cli_runner("connection", "delete", connection_name, check=False)

    def test_list_connections_shows_mssql(self, mssql_connection, cli_runner):
        """Test that connection list shows SQL Server connections correctly."""
        result = cli_runner("connection", "list")
        assert result.returncode == 0
        assert mssql_connection in result.stdout
        assert "SQL Server" in result.stdout

    def test_query_mssql_select(self, mssql_connection, cli_runner):
        """Test executing SELECT query on SQL Server."""
        result = cli_runner(
            "query",
            "-c", mssql_connection,
            "-q", "SELECT * FROM test_users ORDER BY id",
        )
        assert result.returncode == 0
        assert "Alice" in result.stdout
        assert "Bob" in result.stdout
        assert "Charlie" in result.stdout
        assert "3 row(s) returned" in result.stdout

    def test_query_mssql_with_where(self, mssql_connection, cli_runner):
        """Test executing SELECT with WHERE clause on SQL Server."""
        result = cli_runner(
            "query",
            "-c", mssql_connection,
            "-q", "SELECT name, email FROM test_users WHERE id = 1",
        )
        assert result.returncode == 0
        assert "Alice" in result.stdout
        assert "alice@example.com" in result.stdout
        assert "1 row(s) returned" in result.stdout

    def test_query_mssql_top(self, mssql_connection, cli_runner):
        """Test SQL Server specific TOP clause."""
        result = cli_runner(
            "query",
            "-c", mssql_connection,
            "-q", "SELECT TOP 2 * FROM test_users ORDER BY id",
        )
        assert result.returncode == 0
        assert "Alice" in result.stdout
        assert "Bob" in result.stdout
        assert "2 row(s) returned" in result.stdout

    def test_query_mssql_json_format(self, mssql_connection, cli_runner):
        """Test query output in JSON format."""
        result = cli_runner(
            "query",
            "-c", mssql_connection,
            "-q", "SELECT TOP 2 id, name FROM test_users ORDER BY id",
            "--format", "json",
        )
        assert result.returncode == 0

        # Parse JSON output (exclude the row count message)
        lines = result.stdout.strip().split("\n")
        json_output = "\n".join(lines[:-1])
        data = json.loads(json_output)

        assert len(data) == 2
        assert data[0]["name"] == "Alice"
        assert data[1]["name"] == "Bob"

    def test_query_mssql_csv_format(self, mssql_connection, cli_runner):
        """Test query output in CSV format."""
        result = cli_runner(
            "query",
            "-c", mssql_connection,
            "-q", "SELECT TOP 2 id, name FROM test_users ORDER BY id",
            "--format", "csv",
        )
        assert result.returncode == 0
        assert "id,name" in result.stdout
        assert "1,Alice" in result.stdout
        assert "2,Bob" in result.stdout

    def test_query_mssql_view(self, mssql_connection, cli_runner):
        """Test querying a view on SQL Server."""
        result = cli_runner(
            "query",
            "-c", mssql_connection,
            "-q", "SELECT * FROM test_user_emails ORDER BY id",
        )
        assert result.returncode == 0
        assert "Alice" in result.stdout
        assert "3 row(s) returned" in result.stdout

    def test_query_mssql_aggregate(self, mssql_connection, cli_runner):
        """Test aggregate query on SQL Server."""
        result = cli_runner(
            "query",
            "-c", mssql_connection,
            "-q", "SELECT COUNT(*) as user_count FROM test_users",
        )
        assert result.returncode == 0
        assert "3" in result.stdout

    def test_query_mssql_insert(self, mssql_connection, cli_runner):
        """Test INSERT statement on SQL Server."""
        result = cli_runner(
            "query",
            "-c", mssql_connection,
            "-q", "INSERT INTO test_users (id, name, email) VALUES (4, 'David', 'david@example.com')",
        )
        assert result.returncode == 0

        # Verify the insert
        result = cli_runner(
            "query",
            "-c", mssql_connection,
            "-q", "SELECT * FROM test_users WHERE id = 4",
        )
        assert "David" in result.stdout

    def test_delete_mssql_connection(self, mssql_db, cli_runner):
        """Test deleting a SQL Server connection."""
        from .conftest import MSSQL_HOST, MSSQL_PORT, MSSQL_USER, MSSQL_PASSWORD

        connection_name = "test_delete_mssql"

        # Create connection first
        cli_runner(
            "connection", "create",
            "--name", connection_name,
            "--db-type", "mssql",
            "--server", f"{MSSQL_HOST},{MSSQL_PORT}" if MSSQL_PORT != 1433 else MSSQL_HOST,
            "--database", mssql_db,
            "--auth-type", "sql",
            "--username", MSSQL_USER,
            "--password", MSSQL_PASSWORD,
        )

        # Delete it
        result = cli_runner("connection", "delete", connection_name)
        assert result.returncode == 0
        assert "deleted successfully" in result.stdout

        # Verify it's gone
        result = cli_runner("connection", "list")
        assert connection_name not in result.stdout


# =============================================================================
# PostgreSQL Integration Tests
# =============================================================================


class TestPostgreSQLIntegration:
    """Integration tests for PostgreSQL database operations via CLI.

    These tests require a running PostgreSQL instance (via Docker).
    Tests are skipped if PostgreSQL is not available.
    """

    def test_create_postgres_connection(self, postgres_db, cli_runner):
        """Test creating a PostgreSQL connection via CLI."""
        from .conftest import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD

        connection_name = "test_create_postgres"

        try:
            # Create connection
            result = cli_runner(
                "connection", "create",
                "--name", connection_name,
                "--db-type", "postgresql",
                "--server", POSTGRES_HOST,
                "--port", str(POSTGRES_PORT),
                "--database", postgres_db,
                "--username", POSTGRES_USER,
                "--password", POSTGRES_PASSWORD,
            )
            assert result.returncode == 0
            assert "created successfully" in result.stdout

            # Verify it appears in list
            result = cli_runner("connection", "list")
            assert connection_name in result.stdout
            assert "PostgreSQL" in result.stdout

        finally:
            # Cleanup
            cli_runner("connection", "delete", connection_name, check=False)

    def test_list_connections_shows_postgres(self, postgres_connection, cli_runner):
        """Test that connection list shows PostgreSQL connections correctly."""
        result = cli_runner("connection", "list")
        assert result.returncode == 0
        assert postgres_connection in result.stdout
        assert "PostgreSQL" in result.stdout

    def test_query_postgres_select(self, postgres_connection, cli_runner):
        """Test executing SELECT query on PostgreSQL."""
        result = cli_runner(
            "query",
            "-c", postgres_connection,
            "-q", "SELECT * FROM test_users ORDER BY id",
        )
        assert result.returncode == 0
        assert "Alice" in result.stdout
        assert "Bob" in result.stdout
        assert "Charlie" in result.stdout
        assert "3 row(s) returned" in result.stdout

    def test_query_postgres_with_where(self, postgres_connection, cli_runner):
        """Test executing SELECT with WHERE clause on PostgreSQL."""
        result = cli_runner(
            "query",
            "-c", postgres_connection,
            "-q", "SELECT name, email FROM test_users WHERE id = 1",
        )
        assert result.returncode == 0
        assert "Alice" in result.stdout
        assert "alice@example.com" in result.stdout
        assert "1 row(s) returned" in result.stdout

    def test_query_postgres_limit(self, postgres_connection, cli_runner):
        """Test PostgreSQL LIMIT clause."""
        result = cli_runner(
            "query",
            "-c", postgres_connection,
            "-q", "SELECT * FROM test_users ORDER BY id LIMIT 2",
        )
        assert result.returncode == 0
        assert "Alice" in result.stdout
        assert "Bob" in result.stdout
        assert "2 row(s) returned" in result.stdout

    def test_query_postgres_json_format(self, postgres_connection, cli_runner):
        """Test query output in JSON format."""
        result = cli_runner(
            "query",
            "-c", postgres_connection,
            "-q", "SELECT id, name FROM test_users ORDER BY id LIMIT 2",
            "--format", "json",
        )
        assert result.returncode == 0

        # Parse JSON output (exclude the row count message)
        lines = result.stdout.strip().split("\n")
        json_output = "\n".join(lines[:-1])
        data = json.loads(json_output)

        assert len(data) == 2
        assert data[0]["name"] == "Alice"
        assert data[1]["name"] == "Bob"

    def test_query_postgres_csv_format(self, postgres_connection, cli_runner):
        """Test query output in CSV format."""
        result = cli_runner(
            "query",
            "-c", postgres_connection,
            "-q", "SELECT id, name FROM test_users ORDER BY id LIMIT 2",
            "--format", "csv",
        )
        assert result.returncode == 0
        assert "id,name" in result.stdout
        assert "1,Alice" in result.stdout
        assert "2,Bob" in result.stdout

    def test_query_postgres_view(self, postgres_connection, cli_runner):
        """Test querying a view on PostgreSQL."""
        result = cli_runner(
            "query",
            "-c", postgres_connection,
            "-q", "SELECT * FROM test_user_emails ORDER BY id",
        )
        assert result.returncode == 0
        assert "Alice" in result.stdout
        assert "3 row(s) returned" in result.stdout

    def test_query_postgres_aggregate(self, postgres_connection, cli_runner):
        """Test aggregate query on PostgreSQL."""
        result = cli_runner(
            "query",
            "-c", postgres_connection,
            "-q", "SELECT COUNT(*) as user_count FROM test_users",
        )
        assert result.returncode == 0
        assert "3" in result.stdout

    def test_query_postgres_insert(self, postgres_connection, cli_runner):
        """Test INSERT statement on PostgreSQL."""
        result = cli_runner(
            "query",
            "-c", postgres_connection,
            "-q", "INSERT INTO test_users (id, name, email) VALUES (4, 'David', 'david@example.com')",
        )
        assert result.returncode == 0

        # Verify the insert
        result = cli_runner(
            "query",
            "-c", postgres_connection,
            "-q", "SELECT * FROM test_users WHERE id = 4",
        )
        assert "David" in result.stdout

    def test_delete_postgres_connection(self, postgres_db, cli_runner):
        """Test deleting a PostgreSQL connection."""
        from .conftest import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD

        connection_name = "test_delete_postgres"

        # Create connection first
        cli_runner(
            "connection", "create",
            "--name", connection_name,
            "--db-type", "postgresql",
            "--server", POSTGRES_HOST,
            "--port", str(POSTGRES_PORT),
            "--database", postgres_db,
            "--username", POSTGRES_USER,
            "--password", POSTGRES_PASSWORD,
        )

        # Delete it
        result = cli_runner("connection", "delete", connection_name)
        assert result.returncode == 0
        assert "deleted successfully" in result.stdout

        # Verify it's gone
        result = cli_runner("connection", "list")
        assert connection_name not in result.stdout


# =============================================================================
# MySQL Integration Tests
# =============================================================================


class TestMySQLIntegration:
    """Integration tests for MySQL database operations via CLI.

    These tests require a running MySQL instance (via Docker).
    Tests are skipped if MySQL is not available.
    """

    def test_create_mysql_connection(self, mysql_db, cli_runner):
        """Test creating a MySQL connection via CLI."""
        from .conftest import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD

        connection_name = "test_create_mysql"

        try:
            # Create connection
            result = cli_runner(
                "connection", "create",
                "--name", connection_name,
                "--db-type", "mysql",
                "--server", MYSQL_HOST,
                "--port", str(MYSQL_PORT),
                "--database", mysql_db,
                "--username", MYSQL_USER,
                "--password", MYSQL_PASSWORD,
            )
            assert result.returncode == 0
            assert "created successfully" in result.stdout

            # Verify it appears in list
            result = cli_runner("connection", "list")
            assert connection_name in result.stdout
            assert "MySQL" in result.stdout

        finally:
            # Cleanup
            cli_runner("connection", "delete", connection_name, check=False)

    def test_list_connections_shows_mysql(self, mysql_connection, cli_runner):
        """Test that connection list shows MySQL connections correctly."""
        result = cli_runner("connection", "list")
        assert result.returncode == 0
        assert mysql_connection in result.stdout
        assert "MySQL" in result.stdout

    def test_query_mysql_select(self, mysql_connection, cli_runner):
        """Test executing SELECT query on MySQL."""
        result = cli_runner(
            "query",
            "-c", mysql_connection,
            "-q", "SELECT * FROM test_users ORDER BY id",
        )
        assert result.returncode == 0
        assert "Alice" in result.stdout
        assert "Bob" in result.stdout
        assert "Charlie" in result.stdout
        assert "3 row(s) returned" in result.stdout

    def test_query_mysql_with_where(self, mysql_connection, cli_runner):
        """Test executing SELECT with WHERE clause on MySQL."""
        result = cli_runner(
            "query",
            "-c", mysql_connection,
            "-q", "SELECT name, email FROM test_users WHERE id = 1",
        )
        assert result.returncode == 0
        assert "Alice" in result.stdout
        assert "alice@example.com" in result.stdout
        assert "1 row(s) returned" in result.stdout

    def test_query_mysql_limit(self, mysql_connection, cli_runner):
        """Test MySQL LIMIT clause."""
        result = cli_runner(
            "query",
            "-c", mysql_connection,
            "-q", "SELECT * FROM test_users ORDER BY id LIMIT 2",
        )
        assert result.returncode == 0
        assert "Alice" in result.stdout
        assert "Bob" in result.stdout
        assert "2 row(s) returned" in result.stdout

    def test_query_mysql_json_format(self, mysql_connection, cli_runner):
        """Test query output in JSON format."""
        result = cli_runner(
            "query",
            "-c", mysql_connection,
            "-q", "SELECT id, name FROM test_users ORDER BY id LIMIT 2",
            "--format", "json",
        )
        assert result.returncode == 0

        # Parse JSON output (exclude the row count message)
        lines = result.stdout.strip().split("\n")
        json_output = "\n".join(lines[:-1])
        data = json.loads(json_output)

        assert len(data) == 2
        assert data[0]["name"] == "Alice"
        assert data[1]["name"] == "Bob"

    def test_query_mysql_csv_format(self, mysql_connection, cli_runner):
        """Test query output in CSV format."""
        result = cli_runner(
            "query",
            "-c", mysql_connection,
            "-q", "SELECT id, name FROM test_users ORDER BY id LIMIT 2",
            "--format", "csv",
        )
        assert result.returncode == 0
        assert "id,name" in result.stdout
        assert "1,Alice" in result.stdout
        assert "2,Bob" in result.stdout

    def test_query_mysql_view(self, mysql_connection, cli_runner):
        """Test querying a view on MySQL."""
        result = cli_runner(
            "query",
            "-c", mysql_connection,
            "-q", "SELECT * FROM test_user_emails ORDER BY id",
        )
        assert result.returncode == 0
        assert "Alice" in result.stdout
        assert "3 row(s) returned" in result.stdout

    def test_query_mysql_aggregate(self, mysql_connection, cli_runner):
        """Test aggregate query on MySQL."""
        result = cli_runner(
            "query",
            "-c", mysql_connection,
            "-q", "SELECT COUNT(*) as user_count FROM test_users",
        )
        assert result.returncode == 0
        assert "3" in result.stdout

    def test_query_mysql_insert(self, mysql_connection, cli_runner):
        """Test INSERT statement on MySQL."""
        result = cli_runner(
            "query",
            "-c", mysql_connection,
            "-q", "INSERT INTO test_users (id, name, email) VALUES (4, 'David', 'david@example.com')",
        )
        assert result.returncode == 0

        # Verify the insert
        result = cli_runner(
            "query",
            "-c", mysql_connection,
            "-q", "SELECT * FROM test_users WHERE id = 4",
        )
        assert "David" in result.stdout

    def test_delete_mysql_connection(self, mysql_db, cli_runner):
        """Test deleting a MySQL connection."""
        from .conftest import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD

        connection_name = "test_delete_mysql"

        # Create connection first
        cli_runner(
            "connection", "create",
            "--name", connection_name,
            "--db-type", "mysql",
            "--server", MYSQL_HOST,
            "--port", str(MYSQL_PORT),
            "--database", mysql_db,
            "--username", MYSQL_USER,
            "--password", MYSQL_PASSWORD,
        )

        # Delete it
        result = cli_runner("connection", "delete", connection_name)
        assert result.returncode == 0
        assert "deleted successfully" in result.stdout

        # Verify it's gone
        result = cli_runner("connection", "list")
        assert connection_name not in result.stdout


# =============================================================================
# Cross-Database Tests
# =============================================================================


class TestCrossDatabaseFeatures:
    """Tests that verify consistent behavior across database types."""

    def test_both_connection_types_in_list(self, sqlite_connection, mssql_connection, cli_runner):
        """Test that both SQLite and MSSQL connections appear in list."""
        result = cli_runner("connection", "list")
        assert result.returncode == 0
        assert sqlite_connection in result.stdout
        assert mssql_connection in result.stdout
        assert "SQLite" in result.stdout
        assert "SQL Server" in result.stdout

    def test_query_different_databases_sequentially(
        self, sqlite_connection, mssql_connection, cli_runner
    ):
        """Test querying different database types in sequence."""
        # Query SQLite
        result = cli_runner(
            "query",
            "-c", sqlite_connection,
            "-q", "SELECT COUNT(*) FROM test_users",
        )
        assert result.returncode == 0
        assert "3" in result.stdout

        # Query MSSQL
        result = cli_runner(
            "query",
            "-c", mssql_connection,
            "-q", "SELECT COUNT(*) FROM test_users",
        )
        assert result.returncode == 0
        assert "3" in result.stdout
