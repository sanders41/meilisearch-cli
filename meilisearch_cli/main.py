from __future__ import annotations

import sys
from typing import Any, List, Optional

from meilisearch import Client
from meilisearch.errors import MeiliSearchApiError
from rich.console import Console, Group
from rich.panel import Panel
from rich.traceback import install
from typer import Argument, Option, Typer

from meilisearch_cli import documents, dump, index
from meilisearch_cli._config import MASTER_KEY_HELP_MESSAGE, PANEL_BORDER_COLOR, URL_HELP_MESSAGE
from meilisearch_cli._helpers import (
    create_client,
    create_panel,
    handle_index_meilisearch_api_error,
    set_search_param,
)

install()
console = Console()
app = Typer()
app.add_typer(documents.app, name="documents", help="Manage documents in an index.")
app.add_typer(dump.app, name="dump", help="Create and get status of dumps.")
app.add_typer(index.app, name="index", help="Manage indexes")


@app.command()
def api_docs_link() -> None:
    """Gives a clickable link to the MeiliSearch API documenation."""

    console.print("https://docs.meilisearch.com/reference/api/")


@app.command()
def docs_link() -> None:
    """Gives a clickable link to the MeiliSearch documenation."""

    console.print("https://docs.meilisearch.com/")


@app.command()
def get_keys(
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Gets the public and private keys"""

    client = create_client(url, master_key)
    with console.status("Getting keys..."):
        keys = client.get_keys()
        panel = create_panel(keys, title="Keys")

    console.print(panel)


@app.command()
def get_version(
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Gets the MeiliSearch version information."""

    client = create_client(url, master_key)
    with console.status("Getting version..."):
        version = client.get_version()
        panel = create_panel(version, title="Version Information")

    console.print(panel)


@app.command()
def health(
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
) -> None:
    """Checks the status of the server."""

    if not url:
        console.print(
            "A value for [yellow bold]--url[/yellow bold] has to either be provied or available in the [yellow bold]MEILI_HTTP_ADDR[/yellow bold] environment variable",
            style="red",
        )
        sys.exit()

    client = Client(url)
    with console.status("Getting server status..."):
        health = client.health()
        panel = create_panel(health, title="Server Health")

    console.print(panel)


@app.command()
def search(
    index: str = Argument(..., help="The name of the index from which to retrieve the settings"),
    query: str = Argument(..., help="The query string"),
    offset: Optional[int] = Option(None, help="The number of documents to skip"),
    limit: Optional[int] = Option(None, help="The maximum number of documents to return"),
    filter: Optional[List[str]] = Option(None, help="Filter queries by an attribute value"),
    facets_distribution: Optional[List[str]] = Option(
        None, help="Facets for which to retrieve the matching count"
    ),
    attributes_to_retrieve: Optional[List[str]] = Option(
        None, help="Attributes to display in the returned documents"
    ),
    attributes_to_crop: Optional[List[str]] = Option(
        None, help="Attributes whose values have to be cropped"
    ),
    crop_length: Optional[int] = Option(None, help="Length used to crop field values"),
    attributes_to_hightlight: Optional[List[str]] = Option(
        None, help="Attributes whose values will contain highlighted matching terms"
    ),
    matches: bool = Option(
        False,
        help="Defines whether an object that contains information about the matches should be returned or not",
    ),
    sort: Optional[List[str]] = Option(
        None, help="Sort search results according to the attributes"
    ),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Perform a search."""

    client = create_client(url, master_key)
    search_params: dict[str, Any] = {}

    set_search_param(search_params, offset, "offset")
    set_search_param(search_params, limit, "limit")
    set_search_param(search_params, filter, "filter")
    set_search_param(search_params, facets_distribution, "facetsDistribution")
    set_search_param(search_params, attributes_to_retrieve, "attributesToRetrieve")
    set_search_param(search_params, attributes_to_crop, "attributesToCrop")
    set_search_param(search_params, crop_length, "cropLength")
    set_search_param(search_params, attributes_to_hightlight, "attributesToHighlight")
    set_search_param(search_params, matches, "matches")
    set_search_param(search_params, sort, "sort")

    try:
        with console.status("Searching..."):
            if search_params:
                search_results = client.index(index).search(query, search_params)
            else:
                search_results = client.index(index).search(query)

            hits_panel = create_panel(search_results["hits"], title="Hits", fit=False)
            del search_results["hits"]
            info_panel = create_panel(search_results, title="Information", fit=False)
            panel_group = Group(info_panel, hits_panel)
            panel = Panel(panel_group, title="Search Results", border_style=PANEL_BORDER_COLOR)

        console.print(panel)
    except MeiliSearchApiError as e:
        handle_index_meilisearch_api_error(e, index)


if __name__ == "__main__":
    raise SystemExit(app())
