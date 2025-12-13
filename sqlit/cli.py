#!/usr/bin/env python3
"""sqlit - A terminal UI for SQL databases."""

from __future__ import annotations

import argparse
import sys

from .config import AuthType, DatabaseType


def main() -> int:
    """Entry point for the CLI."""
    parser = argparse.ArgumentParser(
        prog="sqlit",
        description="A terminal UI for SQL Server, PostgreSQL, MySQL, and SQLite databases",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Connection commands
    conn_parser = subparsers.add_parser("connection", help="Manage connections")
    conn_subparsers = conn_parser.add_subparsers(
        dest="conn_command", help="Connection commands"
    )

    # connection list
    conn_subparsers.add_parser("list", help="List all saved connections")

    # connection create
    create_parser = conn_subparsers.add_parser("create", help="Create a new connection")
    create_parser.add_argument("--name", "-n", required=True, help="Connection name")
    create_parser.add_argument(
        "--db-type",
        "-t",
        default="mssql",
        choices=[t.value for t in DatabaseType],
        help="Database type (default: mssql)",
    )
    # Server-based database options (SQL Server, PostgreSQL, MySQL)
    create_parser.add_argument("--server", "-s", help="Server address")
    create_parser.add_argument("--port", "-P", help="Port (default: 1433/5432/3306)")
    create_parser.add_argument(
        "--database", "-d", default="", help="Database name (empty = browse all)"
    )
    create_parser.add_argument("--username", "-u", help="Username")
    create_parser.add_argument("--password", "-p", help="Password")
    # SQL Server specific options
    create_parser.add_argument(
        "--auth-type",
        "-a",
        default="sql",
        choices=[t.value for t in AuthType],
        help="Authentication type (SQL Server only, default: sql)",
    )
    # SQLite options
    create_parser.add_argument("--file-path", help="Database file path (SQLite only)")

    # connection edit
    edit_parser = conn_subparsers.add_parser("edit", help="Edit an existing connection")
    edit_parser.add_argument("connection_name", help="Name of connection to edit")
    edit_parser.add_argument("--name", "-n", help="New connection name")
    # Server-based database options (SQL Server, PostgreSQL, MySQL)
    edit_parser.add_argument("--server", "-s", help="Server address")
    edit_parser.add_argument("--port", "-P", help="Port")
    edit_parser.add_argument("--database", "-d", help="Database name")
    edit_parser.add_argument("--username", "-u", help="Username")
    edit_parser.add_argument("--password", "-p", help="Password")
    # SQL Server specific options
    edit_parser.add_argument(
        "--auth-type",
        "-a",
        choices=[t.value for t in AuthType],
        help="Authentication type (SQL Server only)",
    )
    # SQLite options
    edit_parser.add_argument("--file-path", help="Database file path (SQLite only)")

    # connection delete
    delete_parser = conn_subparsers.add_parser("delete", help="Delete a connection")
    delete_parser.add_argument("connection_name", help="Name of connection to delete")

    # query command
    query_parser = subparsers.add_parser("query", help="Execute a SQL query")
    query_parser.add_argument(
        "--connection", "-c", required=True, help="Connection name to use"
    )
    query_parser.add_argument(
        "--database", "-d", help="Database to query (overrides connection default)"
    )
    query_parser.add_argument("--query", "-q", help="SQL query to execute")
    query_parser.add_argument("--file", "-f", help="SQL file to execute")
    query_parser.add_argument(
        "--format",
        "-o",
        default="table",
        choices=["table", "csv", "json"],
        help="Output format (default: table)",
    )

    args = parser.parse_args()

    # No command = launch TUI
    if args.command is None:
        from .app import SSMSTUI

        app = SSMSTUI()
        app.run()
        return 0

    # Import commands lazily to speed up --help
    from .commands import (
        cmd_connection_create,
        cmd_connection_delete,
        cmd_connection_edit,
        cmd_connection_list,
        cmd_query,
    )

    # Handle connection commands
    if args.command == "connection":
        if args.conn_command == "list":
            return cmd_connection_list(args)
        elif args.conn_command == "create":
            return cmd_connection_create(args)
        elif args.conn_command == "edit":
            return cmd_connection_edit(args)
        elif args.conn_command == "delete":
            return cmd_connection_delete(args)
        else:
            conn_parser.print_help()
            return 1

    # Handle query command
    if args.command == "query":
        return cmd_query(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
