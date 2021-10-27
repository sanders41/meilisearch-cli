from __future__ import annotations

import sys
from pathlib import Path
from time import sleep
from typing import Any, Callable, Generator

from meilisearch import Client
from meilisearch.errors import MeiliSearchApiError
from meilisearch.index import Index
from rich.console import Console, group
from rich.panel import Panel

from meilisearch_cli._config import PANEL_BORDER_COLOR


def create_client(url: str | None, master_key: str | None) -> Client:
    console = Console()

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

    return Client(url, master_key)


def create_panel(
    data: dict[str, Any] | list[dict[str, Any]] | str | None,
    *,
    title: str,
    panel_border_color: str = PANEL_BORDER_COLOR,
    padding: tuple[int, int] = (1, 1),
    fit: bool = True,
) -> Panel:
    info: Any
    if isinstance(data, str):
        info = data
    elif isinstance(data, list):
        if data:

            @group()
            def get_panels() -> Generator[Any, None, None]:
                for d in data:  # type: ignore
                    yield create_panel(d, title="", fit=False)

            info = get_panels()
        else:
            info = "[]"
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


def handle_index_meilisearch_api_error(error: MeiliSearchApiError, index_name: str) -> None:
    console = Console()

    if error.error_code == "index_already_exists":
        console.print(f"Index [yellow bold]{index_name}[/yellow bold] already exists", style="red")
    elif error.error_code == "index_not_found":
        console.print(f"Index [yellow bold]{index_name}[/yellow bold] not found", style="red")
    elif error.error_code == "primary_key_already_present":
        console.print(
            f"Index {index_name} already contains a primary key, cannot be reset", style="red"
        )
    else:
        raise error


def print_json_parse_error_message(console: Console, json_str: str) -> None:
    console.print(f"Unable to parse [yellow bold]{json_str}[/yellow bold] as JSON", style="red")


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


def set_search_param(search_params: dict[str, Any], param: Any, param_name: str) -> dict[str, Any]:
    if param:
        search_params[param_name] = param

    return search_params


def validate_file_type_and_set_content_type(console: Console, file_path: Path) -> str:
    file_type = file_path.suffix

    if file_type == ".csv":
        return "text/csv"
    elif file_type == ".json":
        return "application/json"
    elif file_type == ".ndjson":
        return "application/x-ndjson"

    console.print(
        f"[yellow bold]{file_type}[/yellow bold] files are not accepted. Only .json, .csv, and .ndjson are accepted",
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
