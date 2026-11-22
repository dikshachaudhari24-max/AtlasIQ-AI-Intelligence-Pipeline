import json
import os
from contextlib import contextmanager
from datetime import datetime
from time import perf_counter

import typer
from rich.console import Console, Group
from rich.live import Live
from rich.spinner import Spinner
from rich.text import Text

from atlasiq.core.progress import ProgressEvent
from atlasiq.ui.render import THEME, Renderer, clean


def supports_symbols(encoding: str | None) -> bool:
    try:
        "●✓◉↻○✕⚠—─→←↓╭╮│".encode(encoding or "ascii")
    except (LookupError, UnicodeError):
        return False
    return True


class Presentation:
    def __init__(self, plain: bool, json_output: bool, no_color: bool, *, automation_json: bool = False):
        if plain and json_output:
            raise typer.BadParameter("Choose --plain or --json.")
        terminal = Console(theme=THEME)
        self.interactive = terminal.is_terminal and not os.environ.get("CI") and not plain and not json_output
        self.json = json_output or (automation_json and not terminal.is_terminal and not plain)
        quiet = plain or no_color or "NO_COLOR" in os.environ or not terminal.is_terminal
        self.console = Console(theme=THEME, no_color=quiet, color_system=None if quiet else "auto", markup=False)
        self.error_console = Console(stderr=True, theme=THEME, no_color=quiet, color_system=None if quiet else "auto")
        self.ascii = plain or not supports_symbols(self.console.encoding)

    def renderer(self) -> Renderer:
        return Renderer(self.ascii, self.console.width)

    def error(self, message: str) -> None:
        if self.json:
            typer.echo(message, err=True)
        else:
            self.error_console.print(self.renderer().panel(Text(clean(message), "atlas.failure"),
                                                         "Operation blocked", style="atlas.failure"))

    def output(self, value, render) -> None:
        if self.json:
            typer.echo(json.dumps(value, indent=2, default=lambda item: (
                item.isoformat() if isinstance(item, datetime) else str(item)
            )))
        else:
            self.console.print(render(self.renderer()))

    @contextmanager
    def execution(self, title: str):
        if not self.interactive or self.json:
            yield None
            return
        start = perf_counter()
        state = {}
        counts = {}
        recent = ProgressEvent("source", "PENDING", "Waiting for application state")
        animation = Spinner("line" if self.ascii else "dots", text="", style="atlas.primary")

        def display():
            renderer = self.renderer()
            detail = Text((clean(recent.source) + "  |  ") if recent.source else "")
            detail.append(f"{perf_counter() - start:.1f}s  |  {clean(recent.detail)}", "atlas.muted")
            animation.text = detail
            return Group(renderer.header(title, detail), renderer.flow(state), renderer.counts(counts),
                         renderer.panel(animation, "Current operation"))

        with Live(console=self.error_console, get_renderable=display, transient=True, refresh_per_second=4,
                  redirect_stdout=False, redirect_stderr=True):
            def observe(event: ProgressEvent):
                nonlocal recent, state, counts
                recent = event
                state = {**state, event.stage: {"state": event.state, "detail": event.detail}}
                counts = {**counts, **event.counts}
            yield observe
