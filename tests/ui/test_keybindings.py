"""UI tests for keybindings."""

from __future__ import annotations

import pytest

from sqlit.app import SSMSTUI
from sqlit.ui.screens import ConnectionPickerScreen


class TestLeaderKeybindings:
    @pytest.mark.asyncio
    async def test_space_c_opens_connection_picker(self):
        app = SSMSTUI()

        async with app.run_test(size=(100, 35)) as pilot:
            await pilot.press("space")
            await pilot.pause()
            await pilot.press("c")
            await pilot.pause()

            has_picker = any(
                isinstance(screen, ConnectionPickerScreen) for screen in app.screen_stack
            )
            assert has_picker
