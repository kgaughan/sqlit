# sqlit

A simple terminal UI for SQL Server, PostgreSQL, MySQL, and SQLite, for those who just want to run some queries.

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

![Demo](demo-query.gif)


## Features

- Fast and intuitive keyboard only control
- Context based help (no need to memorize tons of hot-keys)
- Browse databases, tables, views, and stored procedures
- Execute SQL queries with syntax highlighting
- Vim-style query editing
- SQL autocomplete for tables, columns, and procedures
- Multiple authentication methods (Windows, SQL Server, Entra ID)
- Save and manage connections
- Responsive terminal UI
- CLI mode for scripting and AI agents
- Themes (Tokyo Night, Nord, and more)
- Auto-detects and installs ODBC drivers


## Motivation
I usually do my work in the terminal, but I found myself either having to boot up massively bloated GUI's like SSMS or Vscode for the simple task of merely browsing my databases and doing some queries toward them. For the vast majority of my use cases, I never used any of the advanced features for inspection and debugging that SSMS and other feature-rich clients provide. 

I had the unfortunate situation where doing queries became a pain-point due to the massive operation it is to open SSMS and it's lack of intuitive keyboard only navigation.

The problem got severely worse when I switched to Linux and had to rely on VS CODE's SQL extension to access my database. Something was not right.

I tried to use some existing TUI's for SQL, but they were not intuitive for me and I missed the immediate ease of use that other TUI's such as Lazygit provides.

sqlit is a lightweight SQL Server TUI that is easy to use and beautiful to look at, just connect and query. It's for you that just wants to run queries toward your database without launching applications that eats your ram and takes time to load up. Sqlit is designed to make it easy and enjoyable to access your data, not painful.


## Installation

```bash
pip install sqlit-tui
```

That's it. When you first run sqlit, it will detect if you're missing ODBC drivers and help you install them for your OS (Ubuntu, Fedora, Arch, macOS, etc).

## Usage

```bash
sqlit
```

The keybindings are shown at the bottom of the screen.

### CLI

```bash
# Run a query
sqlit query -c "MyServer" -q "SELECT * FROM Users"

# Output as CSV or JSON
sqlit query -c "MyServer" -q "SELECT * FROM Users" --format csv
sqlit query -c "MyServer" -f "script.sql" --format json

# Manage connections
sqlit connection list
sqlit connection create --name "MyServer" --server "localhost" --auth-type sql
sqlit connection delete "MyServer"
```

## Keybindings

| Key | Action |
|-----|--------|
| `i` | Enter INSERT mode |
| `Esc` | Back to NORMAL mode |
| `e` / `q` / `r` | Focus Explorer / Query / Results |
| `s` | SELECT TOP 100 from table |
| `Ctrl+P` | Command palette |
| `Ctrl+Q` | Quit |
| `?` | Help |

Autocomplete triggers automatically in INSERT mode. Use `Tab` to accept.

You can also receive autocompletion on columns by typing the table name and hitting "."

## Configuration

Connections and settings are stored in `~/.sqlit/`.

## Contributing

### Development Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/Maxteabag/sqlit.git
   cd sqlit
   ```

2. Install in development mode with test dependencies:
   ```bash
   pip install -e ".[dev]"
   ```

### Running Tests

#### SQLite Tests (No Docker Required)

SQLite tests can run without any external dependencies:

```bash
pytest tests/ -v -k sqlite
```

#### Full Test Suite (Requires Docker)

To run the complete test suite including SQL Server, PostgreSQL, and MySQL tests:

1. Start the test database containers:
   ```bash
   docker compose -f docker-compose.test.yml up -d
   ```

2. Wait for the databases to be ready (about 30-45 seconds), then run tests:
   ```bash
   pytest tests/ -v
   ```

You can leave the containers running between test runs - the test fixtures handle database setup/teardown automatically. Stop them when you're done developing:

```bash
docker compose -f docker-compose.test.yml down
```

#### Running Tests for Specific Databases

```bash
pytest tests/ -v -k sqlite      # SQLite only
pytest tests/ -v -k mssql       # SQL Server only
pytest tests/ -v -k PostgreSQL  # PostgreSQL only
pytest tests/ -v -k MySQL       # MySQL only
```

#### Environment Variables

The database tests can be configured with these environment variables:

**SQL Server:**
| Variable | Default | Description |
|----------|---------|-------------|
| `MSSQL_HOST` | `localhost` | SQL Server hostname |
| `MSSQL_PORT` | `1434` | SQL Server port |
| `MSSQL_USER` | `sa` | SQL Server username |
| `MSSQL_PASSWORD` | `TestPassword123!` | SQL Server password |

**PostgreSQL:**
| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | `localhost` | PostgreSQL hostname |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_USER` | `testuser` | PostgreSQL username |
| `POSTGRES_PASSWORD` | `TestPassword123!` | PostgreSQL password |
| `POSTGRES_DATABASE` | `test_sqlit` | PostgreSQL database |

**MySQL:**
| Variable | Default | Description |
|----------|---------|-------------|
| `MYSQL_HOST` | `localhost` | MySQL hostname |
| `MYSQL_PORT` | `3306` | MySQL port |
| `MYSQL_USER` | `root` | MySQL username |
| `MYSQL_PASSWORD` | `TestPassword123!` | MySQL password |
| `MYSQL_DATABASE` | `test_sqlit` | MySQL database |

### CI/CD

The project uses GitHub Actions for continuous integration:

- **Build**: Verifies the package builds on Python 3.10-3.13
- **SQLite Tests**: Runs SQLite integration tests (no external dependencies)
- **SQL Server Tests**: Runs SQL Server integration tests with Docker service
- **PostgreSQL Tests**: Runs PostgreSQL integration tests with Docker service
- **MySQL Tests**: Runs MySQL integration tests with Docker service
- **Full Test Suite**: Runs all tests with all four databases

Pull requests must pass all CI checks before merging.

## License

MIT
