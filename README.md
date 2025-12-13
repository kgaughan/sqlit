# sqlit

A simple terminal UI for SQL Server, for those who just want to run some queries.

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

The problem got severely worse when I switched to Linux and had to rely on VS CODE's SQL extension to access my SQL database. Something was not right.

I tried to use some existing TUI's for SQL, but they were not intuitive for me and I missed the immediate ease of use that other TUI's such as Lazygit provides.

sqlit is a lightweight SQL Server TUI that is easy to use and beautiful to look at, just connect and query. It's for you that just wants to run queries toward your database without launching applications that eats your ram and takes time to load up. Sqlit is designed to make it easy and enjoyable to access your data, not painful.

It also comes with a CLI, so you can manage your connections and access your data programmatically.




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
