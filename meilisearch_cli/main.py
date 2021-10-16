from __future__ import annotations

import json
from functools import partial
from typing import Any, List, Optional

from meilisearch import Client
from meilisearch.errors import MeiliSearchApiError
from rich.console import Console
from typer import Argument, Option, Typer

from meilisearch_cli._helpers import process_settings

console = Console()
app = Typer()


URL_HELP_MESSAGE = "The url to the MeiliSearch instance"
MASTER_KEY_HELP_MESSAGE = "The master key for the MeiliSearch instance"
WAIT_MESSAGE = "If this flag is set the function will wait for MeiliSearch to finish processing the data and return the results. Otherwise the update ID will be returned immediately"


@app.command()
def create_index(
    index: str = Argument(..., help="The name of the index to create"),
    primary_key: Optional[str] = Option(None, "--primary-key", help="The primary key of the index"),
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Create an index."""

    client = Client(url, master_key)
    try:
        with console.status("Creating index..."):
            if primary_key:
                response = client.create_index(index, {"primaryKey": primary_key})
            else:
                response = client.create_index(index)

        index_display = {
            "uid": response.uid,
            "primary_key": response.primary_key,
            "created_at": response.created_at,
            "updated_at": response.updated_at,
        }

        console.print(index_display)
    except MeiliSearchApiError as e:
        if e.error_code == "index_already_exists":
            console.print(f"Index {index} already exists", style="red")
        else:
            raise e


@app.command()
def delete_index(
    index: str = Argument(..., help="The name of the index to delete"),
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Delete an index."""

    client = Client(url, master_key)
    try:
        with console.status("Deleting the index..."):
            response = client.index(index).delete()

        response.raise_for_status()
        console.print(f"Index {index} successfully deleted", style="green")
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index {index} not found", style="red")
        else:
            raise e


@app.command()
def get_index(
    index: str = Argument(..., help="The name of the index to retrieve"),
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Gets a single index."""

    client = Client(url, master_key)
    try:
        with console.status("Getting index..."):
            index = client.get_raw_index(index)

        console.print(index)
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(e.message, style="red")
        else:
            raise e


@app.command()
def get_indexes(
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Get all indexes."""

    client = Client(url, master_key)
    with console.status("Getting indexes..."):
        indexes = client.get_raw_indexes()

    console.print(indexes)


@app.command()
def get_keys(
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Gets the public and private keys"""

    client = Client(url, master_key)
    with console.status("Getting keys..."):
        keys = client.get_keys()

    console.print(keys)


@app.command()
def get_settings(
    index: str = Argument(..., help="The name of the index to retrieve"),
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Get the settings of an index."""

    client = Client(url, master_key)
    try:
        with console.status("Getting settings..."):
            settings = client.index(index).get_settings()

        console.print(settings)
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index {index} not found", style="red")
        else:
            raise e


@app.command()
def get_version(
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Gets the MeiliSearch version information."""

    client = Client(url, master_key)
    with console.status("Getting version..."):
        version = client.get_version()

    console.print(version)


@app.command()
def health(
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Checks the status of the server."""
    client = Client(url, master_key)
    with console.status("Getting server status..."):
        health = client.health()

    console.print(health)


@app.command()
def reset_displayed_attributes(
    index: str = Argument(
        ..., help="The name of the index for which to reset the displayed attributes"
    ),
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Reset displayed attributes of an index."""

    client_index = Client(url, master_key).index(index)
    try:
        with console.status("Resetting displayed attributes..."):
            process_settings(
                client_index,
                client_index.reset_displayed_attributes(),
                client_index.get_displayed_attributes(),
                wait,
                console,
            )
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index {index} not found", style="red")
        else:
            raise e


@app.command()
def reset_distinct_attribute(
    index: str = Argument(
        ..., help="The name of the index for which to reset the distinct attribute"
    ),
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Reset distinct attribute of an index."""

    client_index = Client(url, master_key).index(index)
    try:
        with console.status("Resetting distinct attribute..."):
            process_settings(
                client_index,
                client_index.reset_distinct_attribute(),
                client_index.get_distinct_attribute(),
                wait,
                console,
            )
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index {index} not found", style="red")
        else:
            raise e


@app.command()
def reset_filterable_attributes(
    index: str = Argument(
        ..., help="The name of the index for which to reset the filterable attributes"
    ),
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Reset filterable attributes of an index."""

    client_index = Client(url, master_key).index(index)
    try:
        with console.status("Resetting filterable attributes..."):
            process_settings(
                client_index,
                client_index.reset_filterable_attributes(),
                client_index.get_filterable_attributes(),
                wait,
                console,
            )
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index {index} not found", style="red")
        else:
            raise e


@app.command()
def reset_ranking_rules(
    index: str = Argument(..., help="The name of the index for which to reset the ranking rules"),
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Reset ranking rules of an index."""

    client_index = Client(url, master_key).index(index)
    try:
        with console.status("Resetting ranking rules..."):
            process_settings(
                client_index,
                client_index.reset_ranking_rules(),
                client_index.get_ranking_rules(),
                wait,
                console,
            )
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index {index} not found", style="red")
        else:
            raise e


@app.command()
def reset_searchable_attributes(
    index: str = Argument(
        ..., help="The name of the index for which to reset the searchable attributes"
    ),
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Reset searchable attributes of an index."""

    client_index = Client(url, master_key).index(index)
    try:
        with console.status("Resetting searchable attributes..."):
            process_settings(
                client_index,
                client_index.reset_searchable_attributes(),
                client_index.get_searchable_attributes(),
                wait,
                console,
            )
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index {index} not found", style="red")
        else:
            raise e


@app.command()
def reset_settings(
    index: str = Argument(..., help="The name of the index for which to reset the settings"),
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Reset all settings of an index."""

    client_index = Client(url, master_key).index(index)
    try:
        with console.status("Resetting settings..."):
            process_settings(
                client_index,
                client_index.reset_settings(),
                client_index.get_settings(),
                wait,
                console,
            )
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index {index} not found", style="red")
        else:
            raise e


@app.command()
def reset_stop_words(
    index: str = Argument(..., help="The name of the index for which to reset the stop words"),
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Reset stop words of an index."""

    client_index = Client(url, master_key).index(index)
    try:
        with console.status("Resetting stop words..."):
            process_settings(
                client_index,
                client_index.reset_stop_words(),
                client_index.get_stop_words(),
                wait,
                console,
            )
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index {index} not found", style="red")
        else:
            raise e


@app.command()
def reset_synonyms(
    index: str = Argument(..., help="The name of the index for which to reset the synonums"),
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Reset synonyms of an index."""

    client_index = Client(url, master_key).index(index)
    try:
        with console.status("Resetting synonyms..."):
            process_settings(
                client_index,
                client_index.reset_synonyms(),
                client_index.get_synonyms(),
                wait,
                console,
            )
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index {index} not found", style="red")
        else:
            raise e


@app.command()
def update_displayed_attributes(
    index: str = Argument(
        ..., help="The name of the index for which to update the diplayed attributes"
    ),
    displayed_attributes: List[str] = Argument(..., help="The displayed attributes for the index"),
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Update the displayed attributes of an index."""

    client_index = Client(url, master_key).index(index)
    try:
        with console.status("Updating displayed attributes..."):
            process_settings(
                client_index,
                partial(client_index.update_distinct_attribute, displayed_attributes),
                client_index.get_distinct_attribute(),
                wait,
                console,
            )
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index {index} not found", style="red")
        else:
            raise e


@app.command()
def update_distinct_attribute(
    index: str = Argument(
        ..., help="The name of the index for which to update the distinct attribute"
    ),
    distinct_attribute: str = Argument(..., help="The distinct attribute for the index"),
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Update the distinct attributes of an index."""

    client_index = Client(url, master_key).index(index)
    try:
        with console.status("Updating distinct attribute..."):
            process_settings(
                client_index,
                partial(client_index.update_distinct_attribute, distinct_attribute),
                client_index.get_distinct_attribute(),
                wait,
                console,
            )
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index {index} not found", style="red")
        else:
            raise e


@app.command()
def update_index(
    index: str = Argument(
        ..., help="The name of the index for which the settings should be udpated"
    ),
    primary_key: Optional[str] = Option(None, "--primary-key", help="The primary key of the index"),
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Update an index."""

    client = Client(url, master_key)
    try:
        with console.status("Updating index..."):
            response = client.index(index).update(primaryKey=primary_key)

        index_display = {
            "uid": response.uid,
            "primary_key": response.primary_key,
            "created_at": response.created_at,
            "updated_at": response.updated_at,
        }

        console.print(index_display)
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index {index} not found", style="red")
        elif e.error_code == "primary_key_already_present":
            console.print(
                f"Index {index} already contains a primary key, cannot be reset", style="red"
            )
        else:
            raise e


@app.command()
def update_ranking_rules(
    index: str = Argument(..., help="The name of the index for which to update the ranking rules"),
    ranking_rules: List[str] = Argument(..., help="A list of ranking rules"),
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Update the ranking rules of an index."""

    client_index = Client(url, master_key).index(index)
    try:
        with console.status("Updating ranking rules..."):
            process_settings(
                client_index,
                partial(client_index.update_ranking_rules, ranking_rules),
                client_index.get_ranking_rules(),
                wait,
                console,
            )
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index {index} not found", style="red")
        else:
            raise e


@app.command()
def update_searchable_attributes(
    index: str = Argument(
        ..., help="The name of the index for which to update the searchable attributes"
    ),
    searchable_attributes: List[str] = Argument(..., help="A list of searchable attributes"),
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Update the searchable attributes of an index."""

    client_index = Client(url, master_key).index(index)
    try:
        with console.status("Updating searchable attributes..."):
            process_settings(
                client_index,
                partial(client_index.update_searchable_attributes, searchable_attributes),
                client_index.get_searchable_attributes(),
                wait,
                console,
            )
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index {index} not found", style="red")
        else:
            raise e


@app.command()
def update_settings(
    index: str = Argument(..., help="The name of the index to retrieve"),
    displayed_attributes: List[str] = Option(
        None, "--displayed-attributes", help="Fields displayed in the returned documents"
    ),
    distinct_attribute: str = Option(
        None, "--distinct-attribute", help="The distinct attribute for the index"
    ),
    filterable_attributes: List[str] = Option(
        None, "--filterable-attributes", help="Filterable attributes for the index"
    ),
    ranking_rules: List[str] = Option(
        None, "--ranking-rules", help="The ranking rules for the index"
    ),
    searchable_attributes: List[str] = Option(
        None,
        "--searchable-attributes",
        help="Fields in which to search for matching query words sorted by order of importance",
    ),
    sortable_attributes: List[str] = Option(
        None, "--sortable-attributes", help="The sortable attributes for the index"
    ),
    stop_words: List[str] = Option(None, "--stop-words", help="The stop words for the index"),
    synonyms: str = Argument(
        ..., help="Synonyms for the index. This should contain JSON passed as a string"
    ),
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Update the settings of an index."""

    client_index = Client(url, master_key).index(index)
    try:
        settings: dict[str, Any] = {}

        if displayed_attributes:
            settings["displayedAttributes"] = displayed_attributes
        if distinct_attribute:
            settings["distinctAttribute"] = distinct_attribute
        if filterable_attributes:
            settings["filterableAttributes"] = filterable_attributes
        if ranking_rules:
            settings["rankingRules"] = ranking_rules
        if searchable_attributes:
            settings["searchableAttributes"] = searchable_attributes
        if sortable_attributes:
            settings["sortableAttributes"] = sortable_attributes
        if stop_words:
            settings["stopWords"] = stop_words
        if synonyms:
            settings["synonyms"] = json.loads(synonyms)
        with console.status("Getting settings..."):
            process_settings(
                client_index,
                partial(client_index.update_settings, settings),
                client_index.get_settings(),
                wait,
                console,
            )
    except json.decoder.JSONDecodeError:
        console.print(f"Unable to parse {synonyms} as JSON", style="red")
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index {index} not found", style="red")
        else:
            raise e


@app.command()
def update_sortable_attributes(
    index: str = Argument(
        ..., help="The name of the index for which to update the sortable attributes"
    ),
    sortable_attributes: List[str] = Argument(..., help="A list of sortable attributes"),
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Update the sortable attributes of an index."""

    client_index = Client(url, master_key).index(index)
    try:
        with console.status("Updating sortable attributes..."):
            process_settings(
                client_index,
                partial(client_index.update_sortable_attributes, sortable_attributes),
                client_index.get_sortable_attributes(),
                wait,
                console,
            )
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index {index} not found", style="red")
        else:
            raise e


@app.command()
def update_stop_words(
    index: str = Argument(..., help="The name of the index for which to update the stop words"),
    stop_words: List[str] = Argument(..., help="A list of stop words"),
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Update the stop words of an index."""

    client_index = Client(url, master_key).index(index)
    try:
        with console.status("Updating stop words..."):
            process_settings(
                client_index,
                partial(client_index.update_stop_words, stop_words),
                client_index.get_stop_words(),
                wait,
                console,
            )
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index {index} not found", style="red")
        else:
            raise e


@app.command()
def update_synonyms(
    index: str = Argument(..., help="The name of the index for which to update the synonyms"),
    synonyms: str = Argument(
        ..., help="Synonyms for the index. This should contain JSON passed as a string"
    ),
    url: Optional[str] = Option(None, "--url", envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, "--master-key", envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Update the searchable attributes of an index."""

    client_index = Client(url, master_key).index(index)
    try:
        with console.status("Updating searchable attributes..."):
            process_settings(
                client_index,
                partial(client_index.update_synonyms, synonyms),
                client_index.get_synonyms(),
                wait,
                console,
            )
    except json.decoder.JSONDecodeError:
        console.print(f"Unable to parse {synonyms} as JSON", style="red")
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index {index} not found", style="red")
        else:
            raise e


if __name__ == "__main__":
    raise SystemExit(app())
