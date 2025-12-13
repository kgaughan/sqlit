"""Pytest fixtures for sqlit integration tests."""

from __future__ import annotations

import os
import shutil
import socket
import sqlite3
import subprocess
import tempfile
import time
from pathlib import Path

import pytest


def is_port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    """Check if a TCP port is open."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, socket.timeout):
        return False


def wait_for_port(host: str, port: int, timeout: float = 60.0) -> bool:
    """Wait for a TCP port to become available."""
    start = time.time()
    while time.time() - start < timeout:
        if is_port_open(host, port):
            return True
        time.sleep(1)
    return False


def run_cli(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run sqlit CLI command and return result."""
    cmd = ["python", "-m", "sqlit.cli"] + list(args)
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
    )
    if check and result.returncode != 0:
        # Ignore RuntimeWarning about module import order
        stderr_clean = "\n".join(
            line for line in result.stderr.split("\n")
            if "RuntimeWarning" not in line and "unpredictable behaviour" not in line
        ).strip()
        if stderr_clean:
            raise RuntimeError(f"CLI command failed: {stderr_clean}")
    return result


def cleanup_connection(name: str) -> None:
    """Delete a connection if it exists, ignoring errors."""
    try:
        run_cli("connection", "delete", name, check=False)
    except Exception:
        pass


# =============================================================================
# SQLite Fixtures
# =============================================================================


@pytest.fixture(scope="function")
def sqlite_db_path(tmp_path: Path) -> Path:
    """Create a temporary SQLite database file path."""
    return tmp_path / "test_database.db"


@pytest.fixture(scope="function")
def sqlite_db(sqlite_db_path: Path) -> Path:
    """Create a temporary SQLite database with test data."""
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    # Create test tables
    cursor.execute("""
        CREATE TABLE test_users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE
        )
    """)

    cursor.execute("""
        CREATE TABLE test_products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL NOT NULL,
            stock INTEGER DEFAULT 0
        )
    """)

    # Create test view
    cursor.execute("""
        CREATE VIEW test_user_emails AS
        SELECT id, name, email FROM test_users WHERE email IS NOT NULL
    """)

    # Insert test data
    cursor.executemany(
        "INSERT INTO test_users (id, name, email) VALUES (?, ?, ?)",
        [
            (1, "Alice", "alice@example.com"),
            (2, "Bob", "bob@example.com"),
            (3, "Charlie", "charlie@example.com"),
        ],
    )

    cursor.executemany(
        "INSERT INTO test_products (id, name, price, stock) VALUES (?, ?, ?, ?)",
        [
            (1, "Widget", 9.99, 100),
            (2, "Gadget", 19.99, 50),
            (3, "Gizmo", 29.99, 25),
        ],
    )

    conn.commit()
    conn.close()

    return sqlite_db_path


@pytest.fixture(scope="function")
def sqlite_connection(sqlite_db: Path) -> str:
    """Create a sqlit CLI connection for SQLite and clean up after test."""
    connection_name = f"test_sqlite_{os.getpid()}"

    # Clean up any existing connection with this name
    cleanup_connection(connection_name)

    # Create the connection
    run_cli(
        "connection", "create",
        "--name", connection_name,
        "--db-type", "sqlite",
        "--file-path", str(sqlite_db),
    )

    yield connection_name

    # Cleanup
    cleanup_connection(connection_name)


# =============================================================================
# SQL Server Fixtures
# =============================================================================

# SQL Server connection settings for Docker
MSSQL_HOST = os.environ.get("MSSQL_HOST", "localhost")
MSSQL_PORT = int(os.environ.get("MSSQL_PORT", "1433"))
MSSQL_USER = os.environ.get("MSSQL_USER", "sa")
MSSQL_PASSWORD = os.environ.get("MSSQL_PASSWORD", "TestPassword123!")
MSSQL_DATABASE = os.environ.get("MSSQL_DATABASE", "test_sqlit")


def mssql_available() -> bool:
    """Check if SQL Server is available."""
    return is_port_open(MSSQL_HOST, MSSQL_PORT)


@pytest.fixture(scope="session")
def mssql_server_ready() -> bool:
    """Check if SQL Server is ready and return True/False."""
    if not mssql_available():
        return False

    # Wait a bit for SQL Server to be fully ready
    time.sleep(2)
    return True


@pytest.fixture(scope="function")
def mssql_db(mssql_server_ready: bool) -> str:
    """Set up SQL Server test database."""
    if not mssql_server_ready:
        pytest.skip("SQL Server is not available")

    try:
        import pyodbc
    except ImportError:
        pytest.skip("pyodbc is not installed")

    # Find available driver
    drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
    if not drivers:
        pytest.skip("No SQL Server ODBC driver installed")

    driver = drivers[0]

    # Connect to master to create test database
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={MSSQL_HOST},{MSSQL_PORT};"
        f"DATABASE=master;"
        f"UID={MSSQL_USER};"
        f"PWD={MSSQL_PASSWORD};"
        f"TrustServerCertificate=yes;"
    )

    try:
        conn = pyodbc.connect(conn_str, timeout=10)
        conn.autocommit = True
        cursor = conn.cursor()

        # Drop test database if it exists (separate statements to avoid "connection busy" errors)
        cursor.execute(f"SELECT name FROM sys.databases WHERE name = '{MSSQL_DATABASE}'")
        if cursor.fetchone():
            cursor.execute(f"ALTER DATABASE [{MSSQL_DATABASE}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
            cursor.execute(f"DROP DATABASE [{MSSQL_DATABASE}]")

        # Create test database
        cursor.execute(f"CREATE DATABASE [{MSSQL_DATABASE}]")
        cursor.close()
        conn.close()

        # Connect to test database and create schema
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={MSSQL_HOST},{MSSQL_PORT};"
            f"DATABASE={MSSQL_DATABASE};"
            f"UID={MSSQL_USER};"
            f"PWD={MSSQL_PASSWORD};"
            f"TrustServerCertificate=yes;"
        )
        conn = pyodbc.connect(conn_str, timeout=10)
        cursor = conn.cursor()

        # Create test tables
        cursor.execute("""
            CREATE TABLE test_users (
                id INT PRIMARY KEY,
                name NVARCHAR(100) NOT NULL,
                email NVARCHAR(100) UNIQUE
            )
        """)

        cursor.execute("""
            CREATE TABLE test_products (
                id INT PRIMARY KEY,
                name NVARCHAR(100) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                stock INT DEFAULT 0
            )
        """)

        # Create test view
        cursor.execute("""
            CREATE VIEW test_user_emails AS
            SELECT id, name, email FROM test_users WHERE email IS NOT NULL
        """)

        # Create test stored procedure
        cursor.execute("""
            CREATE PROCEDURE sp_test_get_users
            AS
            BEGIN
                SELECT * FROM test_users ORDER BY id;
            END
        """)

        # Insert test data
        cursor.execute("""
            INSERT INTO test_users (id, name, email) VALUES
            (1, 'Alice', 'alice@example.com'),
            (2, 'Bob', 'bob@example.com'),
            (3, 'Charlie', 'charlie@example.com')
        """)

        cursor.execute("""
            INSERT INTO test_products (id, name, price, stock) VALUES
            (1, 'Widget', 9.99, 100),
            (2, 'Gadget', 19.99, 50),
            (3, 'Gizmo', 29.99, 25)
        """)

        conn.commit()
        cursor.close()
        conn.close()

    except pyodbc.Error as e:
        pytest.skip(f"Failed to setup SQL Server database: {e}")

    yield MSSQL_DATABASE

    # Cleanup: drop test database
    try:
        conn = pyodbc.connect(
            f"DRIVER={{{driver}}};"
            f"SERVER={MSSQL_HOST},{MSSQL_PORT};"
            f"DATABASE=master;"
            f"UID={MSSQL_USER};"
            f"PWD={MSSQL_PASSWORD};"
            f"TrustServerCertificate=yes;",
            timeout=10,
        )
        conn.autocommit = True
        cursor = conn.cursor()
        # Execute each statement separately to avoid "connection busy" errors
        cursor.execute(f"SELECT name FROM sys.databases WHERE name = '{MSSQL_DATABASE}'")
        if cursor.fetchone():
            cursor.execute(f"ALTER DATABASE [{MSSQL_DATABASE}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
            cursor.execute(f"DROP DATABASE [{MSSQL_DATABASE}]")
        cursor.close()
        conn.close()
    except Exception:
        pass


@pytest.fixture(scope="function")
def mssql_connection(mssql_db: str) -> str:
    """Create a sqlit CLI connection for SQL Server and clean up after test."""
    connection_name = f"test_mssql_{os.getpid()}"

    # Clean up any existing connection with this name
    cleanup_connection(connection_name)

    # Create the connection
    run_cli(
        "connection", "create",
        "--name", connection_name,
        "--db-type", "mssql",
        "--server", f"{MSSQL_HOST},{MSSQL_PORT}" if MSSQL_PORT != 1433 else MSSQL_HOST,
        "--database", mssql_db,
        "--auth-type", "sql",
        "--username", MSSQL_USER,
        "--password", MSSQL_PASSWORD,
    )

    yield connection_name

    # Cleanup
    cleanup_connection(connection_name)


# =============================================================================
# PostgreSQL Fixtures
# =============================================================================

# PostgreSQL connection settings for Docker
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.environ.get("POSTGRES_USER", "testuser")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "TestPassword123!")
POSTGRES_DATABASE = os.environ.get("POSTGRES_DATABASE", "test_sqlit")


def postgres_available() -> bool:
    """Check if PostgreSQL is available."""
    return is_port_open(POSTGRES_HOST, POSTGRES_PORT)


@pytest.fixture(scope="session")
def postgres_server_ready() -> bool:
    """Check if PostgreSQL is ready and return True/False."""
    if not postgres_available():
        return False

    # Wait a bit for PostgreSQL to be fully ready
    time.sleep(1)
    return True


@pytest.fixture(scope="function")
def postgres_db(postgres_server_ready: bool) -> str:
    """Set up PostgreSQL test database."""
    if not postgres_server_ready:
        pytest.skip("PostgreSQL is not available")

    try:
        import psycopg2
    except ImportError:
        pytest.skip("psycopg2 is not installed")

    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DATABASE,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            connect_timeout=10,
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Drop tables if they exist and recreate
        cursor.execute("DROP TABLE IF EXISTS test_users CASCADE")
        cursor.execute("DROP TABLE IF EXISTS test_products CASCADE")
        cursor.execute("DROP VIEW IF EXISTS test_user_emails")

        # Create test tables
        cursor.execute("""
            CREATE TABLE test_users (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100) UNIQUE
            )
        """)

        cursor.execute("""
            CREATE TABLE test_products (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                stock INTEGER DEFAULT 0
            )
        """)

        # Create test view
        cursor.execute("""
            CREATE VIEW test_user_emails AS
            SELECT id, name, email FROM test_users WHERE email IS NOT NULL
        """)

        # Insert test data
        cursor.execute("""
            INSERT INTO test_users (id, name, email) VALUES
            (1, 'Alice', 'alice@example.com'),
            (2, 'Bob', 'bob@example.com'),
            (3, 'Charlie', 'charlie@example.com')
        """)

        cursor.execute("""
            INSERT INTO test_products (id, name, price, stock) VALUES
            (1, 'Widget', 9.99, 100),
            (2, 'Gadget', 19.99, 50),
            (3, 'Gizmo', 29.99, 25)
        """)

        conn.close()

    except Exception as e:
        pytest.skip(f"Failed to setup PostgreSQL database: {e}")

    yield POSTGRES_DATABASE

    # Cleanup: drop test tables
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DATABASE,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            connect_timeout=10,
        )
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS test_users CASCADE")
        cursor.execute("DROP TABLE IF EXISTS test_products CASCADE")
        cursor.execute("DROP VIEW IF EXISTS test_user_emails")
        conn.close()
    except Exception:
        pass


@pytest.fixture(scope="function")
def postgres_connection(postgres_db: str) -> str:
    """Create a sqlit CLI connection for PostgreSQL and clean up after test."""
    connection_name = f"test_postgres_{os.getpid()}"

    # Clean up any existing connection with this name
    cleanup_connection(connection_name)

    # Create the connection
    run_cli(
        "connection", "create",
        "--name", connection_name,
        "--db-type", "postgresql",
        "--server", POSTGRES_HOST,
        "--port", str(POSTGRES_PORT),
        "--database", postgres_db,
        "--username", POSTGRES_USER,
        "--password", POSTGRES_PASSWORD,
    )

    yield connection_name

    # Cleanup
    cleanup_connection(connection_name)


# =============================================================================
# MySQL Fixtures
# =============================================================================

# MySQL connection settings for Docker
# Note: We use root user because MySQL's testuser only has localhost access inside the container
MYSQL_HOST = os.environ.get("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "TestPassword123!")
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "test_sqlit")


def mysql_available() -> bool:
    """Check if MySQL is available."""
    return is_port_open(MYSQL_HOST, MYSQL_PORT)


@pytest.fixture(scope="session")
def mysql_server_ready() -> bool:
    """Check if MySQL is ready and return True/False."""
    if not mysql_available():
        return False

    # Wait a bit for MySQL to be fully ready
    time.sleep(1)
    return True


@pytest.fixture(scope="function")
def mysql_db(mysql_server_ready: bool) -> str:
    """Set up MySQL test database."""
    if not mysql_server_ready:
        pytest.skip("MySQL is not available")

    try:
        import mysql.connector
    except ImportError:
        pytest.skip("mysql-connector-python is not installed")

    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            database=MYSQL_DATABASE,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            connection_timeout=10,
        )
        cursor = conn.cursor()

        # Drop tables if they exist and recreate
        cursor.execute("DROP TABLE IF EXISTS test_users")
        cursor.execute("DROP TABLE IF EXISTS test_products")
        cursor.execute("DROP VIEW IF EXISTS test_user_emails")

        # Create test tables
        cursor.execute("""
            CREATE TABLE test_users (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100) UNIQUE
            )
        """)

        cursor.execute("""
            CREATE TABLE test_products (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                stock INT DEFAULT 0
            )
        """)

        # Create test view
        cursor.execute("""
            CREATE VIEW test_user_emails AS
            SELECT id, name, email FROM test_users WHERE email IS NOT NULL
        """)

        # Insert test data
        cursor.execute("""
            INSERT INTO test_users (id, name, email) VALUES
            (1, 'Alice', 'alice@example.com'),
            (2, 'Bob', 'bob@example.com'),
            (3, 'Charlie', 'charlie@example.com')
        """)

        cursor.execute("""
            INSERT INTO test_products (id, name, price, stock) VALUES
            (1, 'Widget', 9.99, 100),
            (2, 'Gadget', 19.99, 50),
            (3, 'Gizmo', 29.99, 25)
        """)

        conn.commit()
        conn.close()

    except Exception as e:
        pytest.skip(f"Failed to setup MySQL database: {e}")

    yield MYSQL_DATABASE

    # Cleanup: drop test tables
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            database=MYSQL_DATABASE,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            connection_timeout=10,
        )
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS test_users")
        cursor.execute("DROP TABLE IF EXISTS test_products")
        cursor.execute("DROP VIEW IF EXISTS test_user_emails")
        conn.commit()
        conn.close()
    except Exception:
        pass


@pytest.fixture(scope="function")
def mysql_connection(mysql_db: str) -> str:
    """Create a sqlit CLI connection for MySQL and clean up after test."""
    connection_name = f"test_mysql_{os.getpid()}"

    # Clean up any existing connection with this name
    cleanup_connection(connection_name)

    # Create the connection
    run_cli(
        "connection", "create",
        "--name", connection_name,
        "--db-type", "mysql",
        "--server", MYSQL_HOST,
        "--port", str(MYSQL_PORT),
        "--database", mysql_db,
        "--username", MYSQL_USER,
        "--password", MYSQL_PASSWORD,
    )

    yield connection_name

    # Cleanup
    cleanup_connection(connection_name)


# =============================================================================
# Utility Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def cli_runner():
    """Provide the CLI runner function."""
    return run_cli
