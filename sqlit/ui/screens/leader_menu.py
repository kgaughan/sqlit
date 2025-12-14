"""Leader menu screen for command shortcuts."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import ModalScreen
from textual.widgets import Static


class LeaderMenuScreen(ModalScreen):
    """Modal screen showing leader key commands."""

    BINDINGS = [
        Binding("escape", "dismiss", "Close", show=False),
        Binding("space", "dismiss", "Close", show=False),
        # View
        Binding("e", "cmd_toggle_explorer", "Toggle Explorer", show=False),
        Binding("f", "cmd_fullscreen", "Maximize", show=False),
        # Actions
        Binding("h", "cmd_help", "Help", show=False),
        Binding("t", "cmd_theme", "Theme", show=False),
        Binding("q", "cmd_quit", "Quit", show=False),
        # Connection (when connected)
        Binding("x", "cmd_disconnect", "Disconnect", show=False),
        # Cancel (when query running)
        Binding("c", "cmd_cancel", "Cancel", show=False),
    ]

    CSS = """
    LeaderMenuScreen {
        align: right bottom;
        background: rgba(0, 0, 0, 0);
        overlay: none;
    }

    #leader-menu {
        width: auto;
        height: auto;
        max-width: 50;
        background: $surface;
        border: solid $primary;
        padding: 1;
        margin: 1 2;
    }
    """

    def __init__(self):
        super().__init__()

    def compose(self) -> ComposeResult:
        app = self.app
        connected = app.current_connection is not None

        lines = []

        # View category
        lines.append("[bold $text-muted]View[/]")
        lines.append("  [bold $warning]e[/] Toggle Explorer")
        lines.append("  [bold $warning]f[/] Toggle Maximize Current Window")
        lines.append("")

        # Actions category
        lines.append("[bold $text-muted]Actions[/]")
        lines.append("  [bold $warning]c[/] Cancel Running Operation")
        lines.append("  [bold $warning]t[/] Change Theme")
        lines.append("  [bold $warning]h[/] Help")
        lines.append("  [bold $warning]q[/] Quit")

        if connected:
            lines.append("")
            lines.append("[bold $text-muted]Connection[/]")
            lines.append("  [bold $warning]x[/] Disconnect")

        lines.append("")
        lines.append("[$primary]Close: <esc>[/]")

        content = "\n".join(lines)
        yield Static(content, id="leader-menu")

    def action_dismiss(self) -> None:
        self.dismiss(None)

    def _run_and_dismiss(self, action_name: str) -> None:
        """Run an app action and dismiss the menu."""
        self.dismiss(action_name)

    def action_cmd_toggle_explorer(self) -> None:
        self._run_and_dismiss("toggle_explorer")

    def action_cmd_fullscreen(self) -> None:
        self._run_and_dismiss("toggle_fullscreen")

    def action_cmd_help(self) -> None:
        self._run_and_dismiss("show_help")

    def action_cmd_theme(self) -> None:
        self._run_and_dismiss("change_theme")

    def action_cmd_quit(self) -> None:
        self._run_and_dismiss("quit")

    def action_cmd_disconnect(self) -> None:
        if self.app.current_connection:
            self._run_and_dismiss("disconnect")

    def action_cmd_cancel(self) -> None:
        self._run_and_dismiss("cancel_operation")
