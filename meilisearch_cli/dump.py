from __future__ import annotations

from typing import Optional

from rich.traceback import install
from typer import Argument, Option, Typer

from meilisearch_cli._config import MASTER_KEY_HELP_MESSAGE, RAW_MESSAGE, URL_HELP_MESSAGE, console
from meilisearch_cli._helpers import create_client, print_panel_or_raw

install()
app = Typer()


@app.command()
def create(
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    raw: bool = Option(False, help=RAW_MESSAGE),
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
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    raw: bool = Option(False, help=RAW_MESSAGE),
) -> None:
    """Gets the status of a dump creation."""

    client = create_client(url, master_key)
    with console.status("Getting dump status..."):
        response = client.get_dump_status(update_id)
        print_panel_or_raw(raw, response, "Dump Status")


if __name__ == "__main__":
    raise SystemExit(app())
