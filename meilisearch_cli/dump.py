from __future__ import annotations

from typing import Optional

from meilisearch import Client
from rich.console import Console
from typer import Argument, Option, Typer

from meilisearch_cli._config import MASTER_KEY_HELP_MESSAGE, URL_HELP_MESSAGE
from meilisearch_cli._helpers import create_panel, verify_url_and_master_key

console = Console()
app = Typer()


@app.command()
def create(
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Trigger the creation of a dump."""

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client = Client(url, master_key)  # type: ignore
    with console.status("Creating dump..."):
        response = client.create_dump()
        panel = create_panel(response, title="Dump")

    console.print(panel)


@app.command()
def get_status(
    update_id: str = Argument(
        ..., help="The update ID of the dump creation for which to get the status"
    ),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Gets the status of a dump creation."""

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client = Client(url, master_key)  # type: ignore
    with console.status("Getting dump status..."):
        response = client.get_dump_status(update_id)
        panel = create_panel(response, title="Dump Status")

    console.print(panel)


if __name__ == "__main__":
    raise SystemExit(app())
