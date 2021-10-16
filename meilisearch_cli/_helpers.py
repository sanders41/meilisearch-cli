from __future__ import annotations

from time import sleep
from typing import Any, Callable

from meilisearch.index import Index
from rich.console import Console


def process_settings(
    index: Index, settings_method: Callable, retrieve_method: Callable, wait: bool, console: Console
) -> None:
    update = settings_method()
    if wait:
        status = wait_for_update(index, update["updateId"], console)
        if status:
            settings = retrieve_method()
            console.print(settings)
    else:
        console.print(update)


def wait_for_update(index: Index, update_id: str, console: Console) -> dict[str, Any] | None:
    while True:
        get_update = index.get_update_status(update_id)

        if get_update["status"] == "failed":
            console.print(get_update)
            return None

        if get_update["status"] not in ["enqueued", "processing"]:
            return get_update

        sleep(0.05)
