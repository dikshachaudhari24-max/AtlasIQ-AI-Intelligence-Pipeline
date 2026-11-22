import re
from datetime import datetime
from typing import Any

from rich import box
from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.theme import Theme

from atlasiq.core.observability import STAGES

THEME = Theme({
    "atlas.primary": "bright_cyan", "atlas.emphasis": "bold bright_cyan",
    "atlas.accent": "cyan", "atlas.heading": "bold bright_white",
    "atlas.muted": "bright_black", "atlas.success": "green", "atlas.warning": "yellow", "atlas.failure": "red",
})
STATUS = {
    "CONNECTED": ("●", "+", "atlas.success"), "COMPLETED": ("✓", "+", "atlas.success"),
    "RUNNING": ("◉", "*", "atlas.primary"), "RETRYING": ("↻", "~", "atlas.warning"),
    "PENDING": ("○", ".", "atlas.muted"), "FAILED": ("✕", "x", "atlas.failure"),
    "BLOCKED": ("⚠", "!", "atlas.warning"), "PARTIAL": ("⚠", "!", "atlas.warning"),
    "SKIPPED": ("—", "-", "atlas.muted"), "DISABLED": ("—", "-", "atlas.muted"),
    "REGISTERED": ("○", ".", "atlas.muted"),
}
ALIASES = {"CLAIMED": "RUNNING", "RETRY": "RETRYING"}
TERMINAL_CONTROLS = re.compile(r"[\x00-\x1f\x7f-\x9f]")
METRICS = (
    ("discovered", "Discovered"), ("fetched", "Raw revisions"), ("validated", "Validated"),
    ("persisted", "Persisted"), ("enriched", "Enriched"), ("queued", "Queued"),
    ("rejected", "Quarantined"), ("failed", "Failed"),
)


def clean(value: Any) -> str:
    return TERMINAL_CONTROLS.sub(" ", str(value)) if value is not None else "-"


def time_label(value: datetime | None) -> str:
    return value.isoformat(timespec="seconds") if value else "Never"


class Renderer:
    def __init__(self, ascii_only: bool, width: int):
        self.ascii = ascii_only
        self.width = width
        self.box = box.ASCII if ascii_only else box.ROUNDED

    def status(self, raw: str) -> Text:
        state = ALIASES.get(raw, raw)
        symbol, fallback, style = STATUS.get(state, STATUS["PENDING"])
        return Text(f"{fallback if self.ascii else symbol} {clean(state).replace('_', ' ').title()}", style=style)

    def panel(self, renderable, title: str, *, style: str = "atlas.accent") -> Panel:
        return Panel(renderable, title=Text(title, "atlas.heading"), title_align="left", box=self.box,
                     border_style=style, padding=(0, 1))

    def header(self, title: str, detail: Text | str) -> Panel:
        heading = Text("ATLASIQ", "atlas.emphasis")
        heading.append("  /  " + title, "atlas.heading")
        return self.panel(Group(heading, detail if isinstance(detail, Text) else Text(detail, "atlas.muted")),
                          "AI Intelligence Pipeline", style="atlas.primary")

    def flow(self, stages: dict) -> Panel:
        nodes = []
        for key, name in STAGES:
            item = stages.get(key) or {"state": "PENDING", "detail": ""}
            node = Text(name + "\n", "atlas.heading")
            node.append_text(self.status(item["state"]))
            if item["detail"]:
                node.append("\n" + clean(item["detail"]), "atlas.muted")
            nodes.append(node)
        right, left, down = ("-->", "<--", "|") if self.ascii else ("──→", "←──", "↓")
        if self.width < 78:
            path = []
            for node in nodes:
                if path:
                    path.append(Text(down, "atlas.primary"))
                path.append(node)
            return self.panel(Group(*path), "Pipeline")
        table = Table.grid(expand=True, padding=(0, 1))
        for width in (None, 3, None, 3, None):
            table.add_column(width=width, ratio=None if width else 1, vertical="middle")
        def arrow(symbol):
            return Text(symbol, "atlas.primary")

        table.add_row(nodes[0], arrow(right), nodes[1], arrow(right), nodes[2])
        table.add_row("", "", "", "", arrow(down))
        table.add_row(nodes[5], arrow(left), nodes[4], arrow(left), nodes[3])
        table.add_row(arrow(down), "", "", "", "")
        table.add_row(nodes[6], arrow(right), nodes[7], arrow(right), nodes[8])
        return self.panel(table, "Pipeline")

    def counts(self, values: dict) -> Panel:
        table = Table.grid(expand=True, padding=(0, 1))
        pairs = 4 if self.width >= 100 else 2 if self.width >= 50 else 1
        for _ in range(pairs):
            table.add_column(ratio=1)
            table.add_column(justify="right", style="atlas.emphasis")
        for offset in range(0, len(METRICS), pairs):
            row = []
            for key, label in METRICS[offset:offset + pairs]:
                row += [label, f"{values[key]:,}" if key in values else ("-" if self.ascii else "—")]
            table.add_row(*row)
        return self.panel(table, "Record activity")

    def sources(self, sources: list[dict]) -> Panel:
        if not sources:
            return self.panel(Text("No sources are registered.", "atlas.muted"), "Source monitor")
        wide = self.width >= 112
        columns = ["Source / adapter", "Category", "Last crawl", "Found", "Writes", "Errors"]
        if wide:
            columns += ["Last run (UTC)", "Last success (UTC)"]
        table = Table(*columns, box=self.box, expand=True, header_style="atlas.heading", border_style="atlas.accent")
        narrow = Table("Source", "Last crawl / counts", box=self.box, expand=True)
        for row in sources:
            identity = Text(clean(row["name"]), "atlas.heading")
            identity.append(f"\n{clean(row['id'])} / {clean(row['adapter_name'])}", "atlas.muted")
            if not row["enabled"]:
                identity.append("\nDisabled", "atlas.warning")
            elif not wide:
                identity.append("\nEnabled", "atlas.muted")
            state = row["last_status"] or ("REGISTERED" if row["enabled"] else "DISABLED")
            if self.width < 75:
                identity.append(f"\n{clean(row['category'])}  |  last {time_label(row['last_run'])}", "atlas.muted")
            elif not wide:
                identity.append(f"\nLast {time_label(row['last_run'])}", "atlas.muted")
            if not wide:
                identity.append(f"\nSuccess {time_label(row['last_success'])}", "atlas.muted")
            if self.width < 58:
                # Keep the status usable on narrow terminals where the full grid cannot wrap comfortably.
                state_text = self.status(state)
                state_text.append(f"\n{row['discovered']} found / {row['persisted']} writes / {row['errors']} errors")
                narrow.add_row(identity, state_text)
            else:
                cells = [identity, Text(clean(row["category"])), self.status(state),
                         str(row["discovered"]), str(row["persisted"]), str(row["errors"])]
                if wide:
                    cells += [time_label(row["last_run"]), time_label(row["last_success"])]
                table.add_row(*cells)
        return self.panel(narrow if self.width < 58 else table, "Source monitor")

    def tasks(self, counts: dict, tasks: list[dict], diagnostics: bool) -> Panel:
        summary = Text()
        for key, count in counts.items():
            if summary:
                summary.append("  |  ", "atlas.muted")
            summary.append_text(self.status(key))
            summary.append(f" {count:,}")
        if not tasks:
            return self.panel(Group(summary, Text("No queued work or task history.", "atlas.muted")), "Task queue")
        table = Table("Task", "State", "Attempts / lease", "Last error", box=self.box, expand=True,
                      header_style="atlas.heading")
        for row in tasks:
            status = "FAILED" if row["processing_result"] == "REJECTED" else row["status"]
            state = self.status(status)
            if row["processing_result"] == "REJECTED":
                state.append(" / Quarantined", "atlas.warning")
            attempts = f"{row['attempts']}/{row['max_attempts']}"
            if row["lease_expired"]:
                attempts += "\nExpired / recoverable"
            elif row["lease_expires_at"]:
                attempts += "\nLease " + time_label(row["lease_expires_at"])
            elif row["next_retry_at"] and row["status"] == "RETRY":
                attempts += "\nRetry " + time_label(row["next_retry_at"])
            label = str(row["id"]) if diagnostics else str(row["id"])[:8]
            if diagnostics and row["lease_owner"]:
                attempts += "\nWorker " + clean(row["lease_owner"])
            error = clean(row["last_error"])
            if not diagnostics and row["last_error"]:
                error = error.replace("_", " ").capitalize()
            table.add_row(Text(f"{label}\n{clean(row['stage'])}"), state, Text(attempts), Text(error))
        return self.panel(Group(summary, table), "Task queue")

    def attempts(self, attempts: list[dict]) -> Panel:
        if not attempts:
            return self.panel(Text("No provider attempts recorded.", "atlas.muted"), "Recent provider activity")
        lines = []
        for attempt in attempts:
            line = Text(time_label(attempt["created_at"]) + "  ", "atlas.muted")
            line.append(f"{clean(attempt['provider'])} / {clean(attempt['model_name'])}  ")
            line.append(clean(attempt["error_code"] or attempt["outcome"]), (
                "atlas.failure" if attempt["error_code"] else "atlas.success" if attempt["outcome"] == "ACCEPTED" else ""
            ))
            if attempt["latency_ms"] is not None:
                line.append(f"  {attempt['latency_ms']} ms", "atlas.muted")
            lines.append(line)
        return self.panel(Group(*lines), "Recent provider activity")

    def dashboard(self, data: dict, view: str, diagnostics: bool = False) -> Group:
        detail = Text(f"Environment {data['environment'].upper()}  |  PostgreSQL ")
        detail.append_text(self.status(data["database"]))
        detail.append(f"  |  {len(data['sources'])} sources", "atlas.muted")
        panels = [self.header("Operations", detail)]
        if view in {"overview", "pipeline"}:
            panels.extend((self.flow(data["stages"]), self.counts(data["metrics"])))
        if view in {"overview", "sources"}:
            panels.append(self.sources(data["sources"]))
        if view == "tasks":
            panels.append(self.tasks(data["queue"], data["tasks"], diagnostics))
        if view == "overview":
            latest = sorted((row for row in data["sources"] if row["last_run"]),
                            key=lambda row: row["last_run"], reverse=True)[:5]
            lines = []
            for row in latest:
                line = Text(time_label(row["last_run"]) + "  ", "atlas.muted")
                line.append(clean(row["id"]) + "  ")
                line.append_text(self.status(row["last_status"]))
                if row["failure_summary"]:
                    line.append("  " + clean(row["failure_summary"]), "atlas.warning")
                lines.append(line)
            if lines:
                panels.append(self.panel(Group(*lines), "Recent crawl activity"))
        if view in {"overview", "pipeline", "tasks"}:
            panels.append(self.attempts(data["attempts"]))
        return Group(*panels)

    def summary(self, title: str, report: dict) -> Panel:
        table = Table.grid(padding=(0, 2), expand=True)
        table.add_column(ratio=1)
        table.add_column(justify="right")
        table.add_row("Result", self.status(report["status"]))
        for key, value in report.items():
            if key == "status" or value is None:
                continue
            if key == "providers":
                value = ", ".join(f"{item['provider']} / {item['model']}" for item in value)
            if type(value) is int:
                value = f"{value:,}"
            table.add_row(Text(key.replace("_", " ").capitalize(), "atlas.muted"), Text(clean(value)))
        return self.panel(table, title)

    def inspection(self, data: dict) -> Group:
        stage = "FAILED" if data["processing_result"] == "REJECTED" else data["status"] or "PENDING"
        overview = Text(clean(data["title"] or data["kind"]), "atlas.heading")
        overview.append(f"\n{data['kind']}  |  {data['record_id']}\nSource {clean(data['source_id'])}\n{clean(data['source_url'])}\n")
        overview.append_text(self.status(stage))
        overview.append(f"\nRaw revision {data['raw_document_id']}  |  Last processed {time_label(data['updated_at'])}")
        overview.append(f"\nRecorded usage  input {clean(data['usage']['input_tokens'])}  output {clean(data['usage']['output_tokens'])}")
        if data["historical"]:
            overview.append("\nHistorical revision", "atlas.warning")
        if data["last_error"]:
            overview.append("\n" + clean(data["last_error"]), "atlas.warning")
        facts = []
        for fact in (data["intelligence"] or {}).get("facts", []):
            line = Text(f"{clean(fact['field']).replace('_', ' ').title()}  ", "atlas.heading")
            line.append(clean(fact["normalized"] or fact["value"]))
            line.append("\n" + clean(fact["quote"]), "atlas.muted")
            line.append(f"\n{clean(fact['provider'])} / {clean(fact['model'])}  |  evidence matched", "atlas.accent")
            if fact.get("model_confidence") is not None:
                line.append(f"  |  model confidence {fact['model_confidence']:.2f}", "atlas.muted")
            facts.append(line)
        details = Group(*facts) if facts else Text("No extracted facts for this revision.", "atlas.muted")
        return Group(self.panel(overview, "Intelligence record"), self.panel(details, "Evidence-backed extraction"),
                     self.attempts(data["attempts"]))
