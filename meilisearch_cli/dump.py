from __future__ import annotations

from typing import Optional

from rich.traceback import install
from typer import Argument, Typer

from meilisearch_cli._config import MASTER_KEY_OPTION, RAW_OPTION, URL_OPTION, console
from meilisearch_cli._helpers import create_client, print_panel_or_raw

install()
app = Typer()


@app.command()
def create(
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    raw: bool = RAW_OPTION,
) -> None:
    """Trigger the creation of a dump."""

    client = create_client(url, master_key)
    with console.status("Creating dump..."):
        response = client.create_dump()
        print_panel_or_raw(raw, response, "Dump")


@app.command()
def get_status(
    update_id: str = Argument(
        ..., help="The update ID of the dump creation for which to get the status"
    ),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    raw: bool = RAW_OPTION,
) -> None:
    """Gets the status of a dump creation."""

    client = create_client(url, master_key)
    with console.status("Getting dump status..."):
        response = client.get_dump_status(update_id)
        print_panel_or_raw(raw, response, "Dump Status")


if __name__ == "__main__":
    raise SystemExit(app())
