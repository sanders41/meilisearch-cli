from __future__ import annotations

import sys
from time import sleep
from typing import Any, Callable

from meilisearch.index import Index
from rich.console import Console
from rich.panel import Panel
from rich.pretty import Pretty


def create_panel(
    data: dict[str, Any] | list[dict[str, Any]] | str | None,
    *,
    title: str,
    panel_border_color: str = "sky_blue2",
    padding: tuple[int, int] = (1, 1),
    fit: bool = True,
) -> Panel:
    info: Any
    if isinstance(data, str):
        info = data
    elif isinstance(data, list):
        info = Pretty(data) if data is not None else ""
    else:
        info = ""
        if data == {}:
            info = "{}"
        elif data is not None:
            for key, value in data.items():
                if info == "":
                    info = f"[green]{key}[/green]: {value}"
                else:
                    info = f"{info}\n[green]{key}[/green]: {value}"

    if fit:
        return Panel.fit(info, title=title, border_style=panel_border_color, padding=padding)

    return Panel(info, title=title, border_style=panel_border_color, padding=padding)


def process_request(
    index: Index,
    request_method: Callable,
    retrieve_method: Callable,
    wait: bool,
    console: Console,
    title: str,
) -> None:
    update = request_method()
    if wait:
        status = False

        if not isinstance(update, list):
            response = wait_for_update(index, update["updateId"], console)
            if response:
                status = True
        else:
            for u in update:
                response = wait_for_update(index, u["updateId"], console)
                if response:
                    status = True

        if status:
            response = retrieve_method()
            panel = create_panel(response, title=title)
    else:
        panel = create_panel(update, title=title)

    console.print(panel)


def verify_url_and_master_key(
    console: Console, url: str | None = None, master_key: str | None = None
) -> None:
    if not url and not master_key:
        console.print(
            "Values for [yellow]--url[/yellow] and [yellow]--master-key[/yellow] have to either be provided or available in the [yellow]MEILI_HTTP_ADDR[/yellow] and [yellow]MEILI_MASTER_KEY[/yellow] environment variables",
            style="red",
        )
        sys.exit()
    elif not url:
        console.print(
            "A value for [yellow]--url[/yellow] has to either be provied or available in the [yellow]MEILI_HTTP_ADDR[/yellow] environment variable",
            style="red",
        )
        sys.exit()
    elif not master_key:
        console.print(
            "A value for [yellow]--master-key[/yellow] has to either be provied or available in the [yellow]MEILI_MASTER_KEY[/yellow] environment variable",
            style="red",
        )
        sys.exit()


def wait_for_update(index: Index, update_id: int, console: Console) -> dict[str, Any] | None:
    while True:
        get_update = index.get_update_status(update_id)

        if get_update["status"] == "failed":
            console.print(get_update)
            return None

        if get_update["status"] not in ["enqueued", "processing"]:
            return get_update

        sleep(0.05)
