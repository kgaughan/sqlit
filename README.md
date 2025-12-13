# sqlit

A simple terminal UI for SQL Server, for those who just want to run some queries.

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

![Demo](demo-query.gif)

You know that mass of software that is SSMS? You open it up and wait... and wait... just to run a simple SELECT query. Meanwhile it's eating up your RAM like there's no tomorrow.

And if you're on Linux? Your only option has been a VS Code extension. Seriously?

All you want is to connect to a database, browse some tables, and run a query. That's it. You don't need the 47 features you've never touched.

sqlit is a lightweight SQL Server client that shows you what you can do at all times. No memorizing keybindings. No digging through menus. Just connect and query.

## Features

- Browse databases, tables, views, and stored procedures
- Execute SQL queries with syntax highlighting
- Vim-style modal editing (because you're in a terminal)
- SQL autocomplete for tables, columns, and procedures
- Multiple authentication methods (Windows, SQL Server, Entra ID)
- Save and manage connections
- CLI mode for scripting and AI agents
- Themes (Tokyo Night, Nord, and more)
- Auto-detects and installs ODBC drivers

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

## License

MIT
