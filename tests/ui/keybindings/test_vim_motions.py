"""UI tests for vim motion keybindings in the query editor."""

from __future__ import annotations

import pytest

from sqlit.core.vim import VimMode
from sqlit.domains.shell.app.main import SSMSTUI

from ..mocks import MockConnectionStore, MockSettingsStore, build_test_services


def _make_app() -> SSMSTUI:
    services = build_test_services(
        connection_store=MockConnectionStore(),
        settings_store=MockSettingsStore({"theme": "tokyo-night"}),
    )
    return SSMSTUI(services=services)


class TestVimMotionKeybindings:
    """Test vim motion keybindings in NORMAL mode."""

    @pytest.mark.asyncio
    async def test_word_motions_w_W_b_B(self) -> None:
        app = _make_app()

        async with app.run_test(size=(100, 35)) as pilot:
            app.action_focus_query()
            await pilot.pause()

            app.query_input.text = "foo-bar baz"
            app.query_input.cursor_location = (0, 0)
            await pilot.pause()

            await pilot.press("w")
            await pilot.pause()
            assert app.query_input.cursor_location == (0, 4)

            await pilot.press("W")
            await pilot.pause()
            assert app.query_input.cursor_location == (0, 8)

            await pilot.press("b")
            await pilot.pause()
            assert app.query_input.cursor_location == (0, 4)

            await pilot.press("B")
            await pilot.pause()
            assert app.query_input.cursor_location == (0, 0)

    @pytest.mark.asyncio
    async def test_word_end_motions_e_E(self) -> None:
        app = _make_app()

        async with app.run_test(size=(100, 35)) as pilot:
            app.action_focus_query()
            await pilot.pause()

            app.query_input.text = "foo-bar baz"
            app.query_input.cursor_location = (0, 0)
            await pilot.pause()

            await pilot.press("e")
            await pilot.pause()
            assert app.query_input.cursor_location == (0, 2)

            app.query_input.cursor_location = (0, 0)
            await pilot.pause()

            await pilot.press("E")
            await pilot.pause()
            assert app.query_input.cursor_location == (0, 6)

    @pytest.mark.asyncio
    async def test_line_motions_0_dollar_G(self) -> None:
        app = _make_app()

        async with app.run_test(size=(100, 35)) as pilot:
            app.action_focus_query()
            await pilot.pause()

            lines = "SELECT * FROM cats\nSELECT * FROM dogs"
            app.query_input.text = lines
            app.query_input.cursor_location = (0, 7)
            await pilot.pause()

            await pilot.press("0")
            await pilot.pause()
            assert app.query_input.cursor_location == (0, 0)

            await pilot.press("$")
            await pilot.pause()
            assert app.query_input.cursor_location == (0, len("SELECT * FROM cats"))

            await pilot.press("G")
            await pilot.pause()
            assert app.query_input.cursor_location == (1, 0)

    @pytest.mark.asyncio
    async def test_find_char_motions_f_F(self) -> None:
        app = _make_app()

        async with app.run_test(size=(100, 35)) as pilot:
            app.action_focus_query()
            await pilot.pause()

            line = "select cat catalog"
            app.query_input.text = line
            app.query_input.cursor_location = (0, 0)
            await pilot.pause()

            await pilot.press("f")
            await pilot.pause()
            await pilot.press("c")
            await pilot.pause()
            assert app.query_input.cursor_location == (0, 4)

            app.query_input.cursor_location = (0, len(line))
            await pilot.pause()

            await pilot.press("F")
            await pilot.pause()
            await pilot.press("c")
            await pilot.pause()
            assert app.query_input.cursor_location == (0, 11)

    @pytest.mark.asyncio
    async def test_till_char_motions_t_T(self) -> None:
        app = _make_app()

        async with app.run_test(size=(100, 35)) as pilot:
            app.action_focus_query()
            await pilot.pause()

            line = "select cat catalog"
            app.query_input.text = line
            app.query_input.cursor_location = (0, 0)
            await pilot.pause()

            await pilot.press("t")
            await pilot.pause()
            await pilot.press("c")
            await pilot.pause()
            assert app.query_input.cursor_location == (0, 3)

            line_back = "abcdeabc"
            app.query_input.text = line_back
            app.query_input.cursor_location = (0, len(line_back) - 1)
            await pilot.pause()

            await pilot.press("T")
            await pilot.pause()
            await pilot.press("a")
            await pilot.pause()
            assert app.query_input.cursor_location == (0, 6)

    @pytest.mark.asyncio
    async def test_matching_bracket_motion(self) -> None:
        app = _make_app()

        async with app.run_test(size=(100, 35)) as pilot:
            app.action_focus_query()
            await pilot.pause()

            app.query_input.text = "(cats)"
            app.query_input.cursor_location = (0, 0)
            await pilot.pause()

            await pilot.press("%")
            await pilot.pause()
            assert app.query_input.cursor_location == (0, 5)

    @pytest.mark.asyncio
    async def test_go_to_first_line_gg(self) -> None:
        app = _make_app()

        async with app.run_test(size=(100, 35)) as pilot:
            app.action_focus_query()
            await pilot.pause()

            app.query_input.text = "SELECT 1\nSELECT 2\nSELECT 3"
            app.query_input.cursor_location = (2, 4)
            await pilot.pause()

            await pilot.press("g", "g")
            await pilot.pause()
            assert app.query_input.cursor_location == (0, 0)

    @pytest.mark.asyncio
    async def test_change_to_line_end_c_dollar(self) -> None:
        app = _make_app()

        async with app.run_test(size=(100, 35)) as pilot:
            app.action_focus_query()
            await pilot.pause()

            app.query_input.text = "SELECT * FROM cats"
            app.query_input.cursor_location = (0, 7)
            await pilot.pause()

            assert app.vim_mode == VimMode.NORMAL

            await pilot.press("c")
            await pilot.pause()
            await pilot.press("$")
            await pilot.pause()

            assert app.query_input.text == "SELECT "
            assert app.query_input.cursor_location == (0, 7)
            assert app.vim_mode == VimMode.INSERT

    @pytest.mark.asyncio
    async def test_delete_word_dw(self) -> None:
        app = _make_app()

        async with app.run_test(size=(100, 35)) as pilot:
            app.action_focus_query()
            await pilot.pause()

            app.query_input.text = "hello world"
            app.query_input.cursor_location = (0, 0)
            await pilot.pause()

            await pilot.press("d", "w")
            await pilot.pause()

            assert app.query_input.text == "world"
            assert app.query_input.cursor_location == (0, 0)
            assert app.vim_mode == VimMode.NORMAL

    @pytest.mark.asyncio
    async def test_yank_to_line_end_y_dollar(self) -> None:
        app = _make_app()

        async with app.run_test(size=(100, 35)) as pilot:
            app.action_focus_query()
            await pilot.pause()

            app.query_input.text = "SELECT * FROM cats"
            app.query_input.cursor_location = (0, 7)
            app._internal_clipboard = ""
            await pilot.pause()

            await pilot.press("y", "$")
            await pilot.pause()

            assert app._internal_clipboard == "* FROM cats"

    @pytest.mark.asyncio
    async def test_delete_to_line_start_d0(self) -> None:
        app = _make_app()

        async with app.run_test(size=(100, 35)) as pilot:
            app.action_focus_query()
            await pilot.pause()

            app.query_input.text = "hello world"
            app.query_input.cursor_location = (0, 6)
            await pilot.pause()

            await pilot.press("d", "0")
            await pilot.pause()

            assert app.query_input.text == "world"
            assert app.query_input.cursor_location == (0, 0)

    @pytest.mark.asyncio
    async def test_yank_to_end_y_G(self) -> None:
        app = _make_app()

        async with app.run_test(size=(100, 35)) as pilot:
            app.action_focus_query()
            await pilot.pause()

            app.query_input.text = "alpha\nbeta\ngamma"
            app.query_input.cursor_location = (1, 2)
            app._internal_clipboard = ""
            await pilot.pause()

            await pilot.press("y", "G")
            await pilot.pause()

            assert app._internal_clipboard == "beta\ngamma"

    @pytest.mark.asyncio
    async def test_delete_inside_word_diw(self) -> None:
        app = _make_app()

        async with app.run_test(size=(100, 35)) as pilot:
            app.action_focus_query()
            await pilot.pause()

            app.query_input.text = "alpha beta"
            app.query_input.cursor_location = (0, 6)
            await pilot.pause()

            await pilot.press("d", "i")
            await pilot.pause()
            await pilot.press("w")
            await pilot.pause()

            assert app.query_input.text == "alpha "
            assert app.vim_mode == VimMode.NORMAL

    @pytest.mark.asyncio
    async def test_delete_around_word_daw(self) -> None:
        app = _make_app()

        async with app.run_test(size=(100, 35)) as pilot:
            app.action_focus_query()
            await pilot.pause()

            app.query_input.text = "alpha beta gamma"
            app.query_input.cursor_location = (0, 6)
            await pilot.pause()

            await pilot.press("d", "a")
            await pilot.pause()
            await pilot.press("w")
            await pilot.pause()

            assert app.query_input.text == "alpha gamma"

    @pytest.mark.asyncio
    async def test_change_inside_word_ciw(self) -> None:
        app = _make_app()

        async with app.run_test(size=(100, 35)) as pilot:
            app.action_focus_query()
            await pilot.pause()

            app.query_input.text = "alpha beta"
            app.query_input.cursor_location = (0, 6)
            await pilot.pause()

            await pilot.press("c", "i")
            await pilot.pause()
            await pilot.press("w")
            await pilot.pause()

            assert app.query_input.text == "alpha "
            assert app.query_input.cursor_location == (0, 6)
            assert app.vim_mode == VimMode.INSERT

    @pytest.mark.asyncio
    async def test_delete_inside_quotes_di_quote(self) -> None:
        app = _make_app()

        async with app.run_test(size=(100, 35)) as pilot:
            app.action_focus_query()
            await pilot.pause()

            app.query_input.text = 'SELECT "cat" AS name'
            app.query_input.cursor_location = (0, 9)
            await pilot.pause()

            await pilot.press("d", "i")
            await pilot.pause()
            await pilot.press('"')
            await pilot.pause()

            assert app.query_input.text == 'SELECT "" AS name'
            assert app.query_input.cursor_location == (0, 8)

    @pytest.mark.asyncio
    async def test_delete_inside_parens_di_paren(self) -> None:
        app = _make_app()

        async with app.run_test(size=(100, 35)) as pilot:
            app.action_focus_query()
            await pilot.pause()

            app.query_input.text = "func(call(arg))"
            app.query_input.cursor_location = (0, 11)
            await pilot.pause()

            await pilot.press("d", "i")
            await pilot.pause()
            await pilot.press("(")
            await pilot.pause()

            assert app.query_input.text == "func(call())"
