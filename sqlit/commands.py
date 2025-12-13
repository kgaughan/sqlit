"""CLI command handlers for sqlit."""

from __future__ import annotations

import json

from .adapters import get_adapter
from .config import (
    AUTH_TYPE_LABELS,
    AuthType,
    ConnectionConfig,
    DATABASE_TYPE_LABELS,
    DatabaseType,
    load_connections,
    save_connections,
)


def cmd_connection_list(args) -> int:
    """List all saved connections."""
    connections = load_connections()
    if not connections:
        print("No saved connections.")
        return 0

    print(f"{'Name':<20} {'Type':<10} {'Connection Info':<40} {'Auth Type':<25}")
    print("-" * 95)
    for conn in connections:
        db_type_label = DATABASE_TYPE_LABELS.get(conn.get_db_type(), conn.db_type)
        if conn.db_type == "sqlite":
            conn_info = conn.file_path[:38] + ".." if len(conn.file_path) > 40 else conn.file_path
            auth_label = "N/A"
        elif conn.db_type in ("postgresql", "mysql"):
            conn_info = f"{conn.server}@{conn.database}" if conn.database else conn.server
            conn_info = conn_info[:38] + ".." if len(conn_info) > 40 else conn_info
            auth_label = f"User: {conn.username}" if conn.username else "N/A"
        else:  # mssql
            conn_info = f"{conn.server}@{conn.database}" if conn.database else conn.server
            conn_info = conn_info[:38] + ".." if len(conn_info) > 40 else conn_info
            auth_label = AUTH_TYPE_LABELS.get(conn.get_auth_type(), conn.auth_type)
        print(
            f"{conn.name:<20} {db_type_label:<10} {conn_info:<40} {auth_label:<25}"
        )
    return 0


def cmd_connection_create(args) -> int:
    """Create a new connection."""
    connections = load_connections()

    if any(c.name == args.name for c in connections):
        print(f"Error: Connection '{args.name}' already exists. Use 'edit' to modify it.")
        return 1

    # Determine database type
    db_type = getattr(args, "db_type", "mssql") or "mssql"
    try:
        DatabaseType(db_type)
    except ValueError:
        valid_types = ", ".join(t.value for t in DatabaseType)
        print(f"Error: Invalid database type '{db_type}'. Valid types: {valid_types}")
        return 1

    if db_type == "sqlite":
        # SQLite connection
        file_path = getattr(args, "file_path", None)
        if not file_path:
            print("Error: --file-path is required for SQLite connections.")
            return 1

        config = ConnectionConfig(
            name=args.name,
            db_type=db_type,
            file_path=file_path,
        )
    elif db_type in ("postgresql", "mysql"):
        # PostgreSQL or MySQL connection
        if not args.server:
            print(f"Error: --server is required for {db_type.upper()} connections.")
            return 1

        default_port = "5432" if db_type == "postgresql" else "3306"
        config = ConnectionConfig(
            name=args.name,
            db_type=db_type,
            server=args.server,
            port=args.port or default_port,
            database=args.database or "",
            username=args.username or "",
            password=args.password or "",
        )
    else:
        # SQL Server connection
        if not args.server:
            print("Error: --server is required for SQL Server connections.")
            return 1

        auth_type_str = getattr(args, "auth_type", "sql") or "sql"
        try:
            auth_type = AuthType(auth_type_str)
        except ValueError:
            valid_types = ", ".join(t.value for t in AuthType)
            print(f"Error: Invalid auth type '{auth_type_str}'. Valid types: {valid_types}")
            return 1

        config = ConnectionConfig(
            name=args.name,
            db_type=db_type,
            server=args.server,
            port=args.port or "1433",
            database=args.database or "",
            username=args.username or "",
            password=args.password or "",
            auth_type=auth_type.value,
            trusted_connection=(auth_type == AuthType.WINDOWS),
        )

    connections.append(config)
    save_connections(connections)
    print(f"Connection '{args.name}' created successfully.")
    return 0


def cmd_connection_edit(args) -> int:
    """Edit an existing connection."""
    connections = load_connections()

    conn_idx = None
    for i, c in enumerate(connections):
        if c.name == args.connection_name:
            conn_idx = i
            break

    if conn_idx is None:
        print(f"Error: Connection '{args.connection_name}' not found.")
        return 1

    conn = connections[conn_idx]

    if args.name:
        if args.name != conn.name and any(c.name == args.name for c in connections):
            print(f"Error: Connection '{args.name}' already exists.")
            return 1
        conn.name = args.name

    # SQL Server fields
    if args.server:
        conn.server = args.server
    if args.port:
        conn.port = args.port
    if args.database:
        conn.database = args.database
    if args.auth_type:
        try:
            auth_type = AuthType(args.auth_type)
            conn.auth_type = auth_type.value
            conn.trusted_connection = auth_type == AuthType.WINDOWS
        except ValueError:
            valid_types = ", ".join(t.value for t in AuthType)
            print(f"Error: Invalid auth type '{args.auth_type}'. Valid types: {valid_types}")
            return 1
    if args.username is not None:
        conn.username = args.username
    if args.password is not None:
        conn.password = args.password

    # SQLite fields
    file_path = getattr(args, "file_path", None)
    if file_path is not None:
        conn.file_path = file_path

    save_connections(connections)
    print(f"Connection '{conn.name}' updated successfully.")
    return 0


def cmd_connection_delete(args) -> int:
    """Delete a connection."""
    connections = load_connections()

    conn_idx = None
    for i, c in enumerate(connections):
        if c.name == args.connection_name:
            conn_idx = i
            break

    if conn_idx is None:
        print(f"Error: Connection '{args.connection_name}' not found.")
        return 1

    deleted = connections.pop(conn_idx)
    save_connections(connections)
    print(f"Connection '{deleted.name}' deleted successfully.")
    return 0


def cmd_query(args) -> int:
    """Execute a SQL query against a connection."""
    connections = load_connections()

    config = None
    for c in connections:
        if c.name == args.connection:
            config = c
            break

    if config is None:
        print(f"Error: Connection '{args.connection}' not found.")
        return 1

    # Override database if specified (only for SQL Server)
    if args.database and config.db_type == "mssql":
        config.database = args.database

    if args.query:
        query = args.query
    elif args.file:
        try:
            with open(args.file, "r", encoding="utf-8") as f:
                query = f.read()
        except FileNotFoundError:
            print(f"Error: File '{args.file}' not found.")
            return 1
        except IOError as e:
            print(f"Error reading file: {e}")
            return 1
    else:
        print("Error: Either --query or --file must be provided.")
        return 1

    try:
        adapter = get_adapter(config.db_type)
        db_conn = adapter.connect(config)

        # Detect query type to avoid executing non-SELECT statements twice
        query_type = query.strip().upper().split()[0] if query.strip() else ""
        is_select_query = query_type in ("SELECT", "WITH", "SHOW", "DESCRIBE", "EXPLAIN", "PRAGMA")

        if is_select_query:
            columns, rows = adapter.execute_query(db_conn, query)
        else:
            columns, rows = [], []

        if columns:
            if args.format == "csv":
                print(",".join(columns))
                for row in rows:
                    print(",".join(str(val) if val is not None else "" for val in row))
            elif args.format == "json":
                result = []
                for row in rows:
                    result.append(
                        dict(
                            zip(
                                columns,
                                [val if val is not None else None for val in row],
                            )
                        )
                    )
                print(json.dumps(result, indent=2, default=str))
            else:
                col_widths = [len(col) for col in columns]
                for row in rows:
                    for i, val in enumerate(row):
                        col_widths[i] = max(
                            col_widths[i], len(str(val) if val is not None else "NULL")
                        )

                header = " | ".join(
                    col.ljust(col_widths[i]) for i, col in enumerate(columns)
                )
                print(header)
                print("-" * len(header))

                for row in rows:
                    row_str = " | ".join(
                        (str(val) if val is not None else "NULL").ljust(col_widths[i])
                        for i, val in enumerate(row)
                    )
                    print(row_str)

            print(f"\n({len(rows)} row(s) returned)")
        else:
            affected = adapter.execute_non_query(db_conn, query)
            print(f"Query executed successfully. Rows affected: {affected}")

        db_conn.close()
        return 0

    except ImportError as e:
        print(f"Error: Required module not installed: {e}")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        return 1
