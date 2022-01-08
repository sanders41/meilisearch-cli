from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Callable, Generator

from meilisearch.client import Client
from meilisearch.config import Config
from meilisearch.errors import MeiliSearchApiError, MeiliSearchError
from meilisearch.index import Index
from meilisearch.task import get_task
from rich.console import group
from rich.panel import Panel

from meilisearch_cli._config import PANEL_BORDER_COLOR, SECONDARY_BORDER_COLOR, console


def check_index_status(config: Config, index_id: str, task_id: int) -> None:
    result = get_task(config, task_id)
    if result.get("error"):
        if result["error"]["code"] == "index_already_exists":
            console.print(f"Index [error_highlight]{index_id}[/] already exists", style="error")
            sys.exit(0)
        elif result["error"]["code"] == "index_not_found":
            console.print(f"Index [error_highlight]{index_id}[/] not found", style="error")
            sys.exit(0)
        else:
            raise MeiliSearchError(result["error"]["message"])


def create_client(url: str | None, master_key: str | None) -> Client:
    if not url and not master_key:
        console.print(
            "Values for [error_highlight]--url[/] and [error_highlight]--master-key[/] have to either be provided or available in the [error_highlight]MEILI_HTTP_ADDR[/] and [error_highlight]MEILI_MASTER_KEY[/] environment variables",
            style="error",
        )
        sys.exit()
    elif not url:
        console.print(
            "A value for [error_highlight]--url[/] has to either be provied or available in the [error_highlight]MEILI_HTTP_ADDR[/] environment variable",
            style="error",
        )
        sys.exit()
    elif not master_key:
        console.print(
            "A value for [error_highlight]--master-key[/] has to either be provied or available in the [error_highlight]MEILI_MASTER_KEY[/] environment variable",
            style="error",
        )
        sys.exit()

    return Client(url, master_key)


def create_panel(
    data: dict[str, Any] | list[dict[str, Any]] | str | None,
    *,
    title: str,
    padding: tuple[int, int] = (1, 1),
    fit: bool = True,
    panel_border_color: str = PANEL_BORDER_COLOR,
) -> Panel:
    info: Any
    if isinstance(data, str):
        info = data
    elif isinstance(data, list):
        if data:

            @group()
            def get_panels() -> Generator[Any, None, None]:
                for d in data:  # type: ignore
                    yield create_panel(
                        d, title="", fit=False, panel_border_color=SECONDARY_BORDER_COLOR
                    )

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
                    info = f"[green]{key}[/]: {value}"
                else:
                    info = f"{info}\n[green]{key}[/]: {value}"

    if fit:
        return Panel.fit(info, title=title, border_style=panel_border_color, padding=padding)

    return Panel(info, title=title, border_style=panel_border_color, padding=padding)


def handle_meilisearch_api_error(error: MeiliSearchApiError, index_name: str) -> None:
    if error.code == "index_not_found":
        console.print(f"Index [error_highlight]{index_name}[/] not found", style="error")
    else:
        raise


def print_json_parse_error_message(json_str: str) -> None:
    console.print(f"Unable to parse [error_highlight]{json_str}[/] as JSON", style="error")


def print_panel_or_raw(
    raw: bool, data: dict[str, Any] | list[dict[str, Any]] | None, panel_title: str
) -> None:
    if raw:
        console.print_json(json.dumps(data, default=str))
    else:
        panel = create_panel(data, title=panel_title)
        console.print(panel)


def process_request(
    index: Index,
    request_method: Callable,
    retrieve_method: Callable,
    wait: bool,
    title: str,
    raw: bool,
) -> None:
    update = request_method()

    if wait:
        if not isinstance(update, list):
            response = index.wait_for_task(update["uid"], timeout_in_ms=600000)
            check_index_status(index.config, index.uid, response["uid"])
            if response["status"] == "failed":
                print_panel_or_raw(raw, response, "Failed")
                sys.exit(1)
        else:
            for u in update:
                response = index.wait_for_task(u["uid"], timeout_in_ms=600000)
                if response["status"] == "failed":
                    print_panel_or_raw(raw, response, "Failed")
                    sys.exit(1)

        response = retrieve_method()
        print_panel_or_raw(raw, response, title)
    else:
        print_panel_or_raw(raw, update, title)


def set_search_param(search_params: dict[str, Any], param: Any, param_name: str) -> dict[str, Any]:
    if param:
        search_params[param_name] = param

    return search_params


def validate_file_type_and_set_content_type(file_path: Path) -> str:
    file_type = file_path.suffix

    if file_type == ".csv":
        return "text/csv"
    elif file_type == ".json":
        return "application/json"
    elif file_type == ".ndjson":
        return "application/x-ndjson"

    console.print(
        f"[error_highlight]{file_type}[/] files are not accepted. Only .json, .csv, and .ndjson are accepted",
        style="error",
    )
    sys.exit()
