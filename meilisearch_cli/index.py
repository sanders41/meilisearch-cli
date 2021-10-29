from __future__ import annotations

import json
from functools import partial
from typing import Any, List, Optional

from meilisearch.errors import MeiliSearchApiError
from rich.traceback import install
from typer import Argument, Option, Typer

from meilisearch_cli._config import MASTER_KEY_HELP_MESSAGE, URL_HELP_MESSAGE, WAIT_MESSAGE, console
from meilisearch_cli._helpers import (
    create_client,
    create_panel,
    handle_index_meilisearch_api_error,
    print_json_parse_error_message,
    process_request,
)

install(show_locals=True)
app = Typer()


@app.command()
def create(
    index: str = Argument(..., help="The name of the index to create"),
    primary_key: Optional[str] = Option(None, help="The primary key of the index"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Create an index."""

    client = create_client(url, master_key)
    try:
        with console.status("Creating index..."):
            if primary_key:
                response = client.create_index(index, {"primaryKey": primary_key})
            else:
                response = client.create_index(index)

        index_dict = response.__dict__
        del index_dict["config"]
        del index_dict["http"]
        panel = create_panel(index_dict, title="Index")

        console.print(panel)
    except MeiliSearchApiError as e:
        handle_index_meilisearch_api_error(e, index)


@app.command()
def delete(
    index: str = Argument(..., help="The name of the index to delete"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Delete an index."""

    client = create_client(url, master_key)
    try:
        with console.status("Deleting the index..."):
            response = client.index(index).delete()

        response.raise_for_status()
        console.print(
            create_panel(
                f"Index {index} successfully deleted",
                title="Delete Index",
            )
        )
    except MeiliSearchApiError as e:
        handle_index_meilisearch_api_error(e, index)


@app.command()
def get(
    index: str = Argument(..., help="The name of the index to retrieve"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Gets a single index."""

    client = create_client(url, master_key)
    try:
        with console.status("Getting index..."):
            returned_index = client.get_raw_index(index)
            panel = create_panel(returned_index, title="Index")

        console.print(panel)
    except MeiliSearchApiError as e:
        handle_index_meilisearch_api_error(e, index)


@app.command()
def get_all(
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Get all indexes."""

    client = create_client(url, master_key)
    with console.status("Getting indexes..."):
        indexes = client.get_raw_indexes()
        panel = create_panel(indexes, title="All Indexes")

    console.print(panel)


@app.command()
def get_primary_key(
    index: str = Argument(..., help="The name of the index from which to retrieve the primary key"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Get the primary key of an index."""

    client = create_client(url, master_key)
    try:
        with console.status("Getting primary key..."):
            primary_key = client.index(index).get_primary_key()
            panel = create_panel(primary_key, title="Primary Key")

        console.print(panel)
    except MeiliSearchApiError as e:
        handle_index_meilisearch_api_error(e, index)


@app.command()
def get_stats(
    index: str = Argument(..., help="The name of the index from which to retrieve the stats"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Get the stats of an index."""

    client = create_client(url, master_key)
    try:
        with console.status("Getting stats..."):
            settings = client.index(index).get_stats()
            panel = create_panel(settings, title="Stats")

        console.print(panel)
    except MeiliSearchApiError as e:
        handle_index_meilisearch_api_error(e, index)


@app.command()
def get_all_update_status(
    index: str = Argument(
        ..., help="The name of the index from which to retrieve the update status"
    ),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Get all update statuses of an index."""

    client = create_client(url, master_key)
    try:
        with console.status("Getting update status..."):
            status = client.index(index).get_all_update_status()
            panel = create_panel(status, title="Update Status")

        console.print(panel)
    except MeiliSearchApiError as e:
        handle_index_meilisearch_api_error(e, index)


@app.command()
def get_settings(
    index: str = Argument(..., help="The name of the index from which to retrieve the settings"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Get the settings of an index."""

    client = create_client(url, master_key)
    try:
        with console.status("Getting settings..."):
            settings = client.index(index).get_settings()
            panel = create_panel(settings, title="Settings")

        console.print(panel)
    except MeiliSearchApiError as e:
        handle_index_meilisearch_api_error(e, index)


@app.command()
def get_update_status(
    index: str = Argument(
        ..., help="The name of the index from which to retrieve the update status"
    ),
    update_id: int = Argument(..., help="The update ID for the update status"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Get the update status of an index."""

    client = create_client(url, master_key)
    try:
        with console.status("Getting update status..."):
            status = client.index(index).get_update_status(update_id)
            panel = create_panel(status, title="Update Status")

        console.print(panel)
    except MeiliSearchApiError as e:
        handle_index_meilisearch_api_error(e, index)


@app.command()
def reset_displayed_attributes(
    index: str = Argument(
        ..., help="The name of the index for which to reset the displayed attributes"
    ),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Reset displayed attributes of an index."""

    client_index = create_client(url, master_key).index(index)
    try:
        with console.status("Resetting displayed attributes..."):
            process_request(
                client_index,
                client_index.reset_displayed_attributes,
                client_index.get_displayed_attributes,
                wait,
                "Reset Displayed Attributes",
            )
    except MeiliSearchApiError as e:
        handle_index_meilisearch_api_error(e, index)


@app.command()
def reset_distinct_attribute(
    index: str = Argument(
        ..., help="The name of the index for which to reset the distinct attribute"
    ),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Reset distinct attribute of an index."""

    client_index = create_client(url, master_key).index(index)
    try:
        with console.status("Resetting distinct attribute..."):
            process_request(
                client_index,
                client_index.reset_distinct_attribute,
                client_index.get_distinct_attribute,
                wait,
                "Reset Distinct Attribute",
            )
    except MeiliSearchApiError as e:
        handle_index_meilisearch_api_error(e, index)


@app.command()
def reset_filterable_attributes(
    index: str = Argument(
        ..., help="The name of the index for which to reset the filterable attributes"
    ),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Reset filterable attributes of an index."""

    client_index = create_client(url, master_key).index(index)
    try:
        with console.status("Resetting filterable attributes..."):
            process_request(
                client_index,
                client_index.reset_filterable_attributes,
                client_index.get_filterable_attributes,
                wait,
                "Reset Filterable Attributes",
            )
    except MeiliSearchApiError as e:
        handle_index_meilisearch_api_error(e, index)


@app.command()
def reset_ranking_rules(
    index: str = Argument(..., help="The name of the index for which to reset the ranking rules"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Reset ranking rules of an index."""

    client_index = create_client(url, master_key).index(index)
    try:
        with console.status("Resetting ranking rules..."):
            process_request(
                client_index,
                client_index.reset_ranking_rules,
                client_index.get_ranking_rules,
                wait,
                "Reset Ranking Rules",
            )
    except MeiliSearchApiError as e:
        handle_index_meilisearch_api_error(e, index)


@app.command()
def reset_searchable_attributes(
    index: str = Argument(
        ..., help="The name of the index for which to reset the searchable attributes"
    ),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Reset searchable attributes of an index."""

    client_index = create_client(url, master_key).index(index)
    try:
        with console.status("Resetting searchable attributes..."):
            process_request(
                client_index,
                client_index.reset_searchable_attributes,
                client_index.get_searchable_attributes,
                wait,
                "Reset Searchable Attributes",
            )
    except MeiliSearchApiError as e:
        handle_index_meilisearch_api_error(e, index)


@app.command()
def reset_settings(
    index: str = Argument(..., help="The name of the index for which to reset the settings"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Reset all settings of an index."""

    client_index = create_client(url, master_key).index(index)
    try:
        with console.status("Resetting settings..."):
            process_request(
                client_index,
                client_index.reset_settings,
                client_index.get_settings,
                wait,
                "Reset Settings",
            )
    except MeiliSearchApiError as e:
        handle_index_meilisearch_api_error(e, index)


@app.command()
def reset_stop_words(
    index: str = Argument(..., help="The name of the index for which to reset the stop words"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Reset stop words of an index."""

    client_index = create_client(url, master_key).index(index)
    try:
        with console.status("Resetting stop words..."):
            process_request(
                client_index,
                client_index.reset_stop_words,
                client_index.get_stop_words,
                wait,
                "Reset Stop Words",
            )
    except MeiliSearchApiError as e:
        handle_index_meilisearch_api_error(e, index)


@app.command()
def reset_synonyms(
    index: str = Argument(..., help="The name of the index for which to reset the synonums"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Reset synonyms of an index."""

    client_index = create_client(url, master_key).index(index)
    try:
        with console.status("Resetting synonyms..."):
            process_request(
                client_index,
                client_index.reset_synonyms,
                client_index.get_synonyms,
                wait,
                "Reset Synonyms",
            )
    except MeiliSearchApiError as e:
        handle_index_meilisearch_api_error(e, index)


@app.command()
def update_displayed_attributes(
    index: str = Argument(
        ..., help="The name of the index for which to update the diplayed attributes"
    ),
    displayed_attributes: List[str] = Argument(..., help="The displayed attributes for the index"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Update the displayed attributes of an index."""

    client_index = create_client(url, master_key).index(index)
    with console.status("Updating displayed attributes..."):
        process_request(
            client_index,
            partial(client_index.update_displayed_attributes, displayed_attributes),
            client_index.get_displayed_attributes,
            wait,
            "Update Displayed Attributes",
        )


@app.command()
def update_distinct_attribute(
    index: str = Argument(
        ..., help="The name of the index for which to update the distinct attribute"
    ),
    distinct_attribute: str = Argument(..., help="The distinct attribute for the index"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Update the distinct attributes of an index."""

    client_index = create_client(url, master_key).index(index)
    with console.status("Updating distinct attribute..."):
        process_request(
            client_index,
            partial(client_index.update_distinct_attribute, distinct_attribute),
            client_index.get_distinct_attribute,
            wait,
            "Update Distinct Attribute",
        )


@app.command()
def update(
    index: str = Argument(
        ..., help="The name of the index for which the settings should be udpated"
    ),
    primary_key: Optional[str] = Option(None, help="The primary key of the index"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Update an index."""

    client = create_client(url, master_key)
    try:
        with console.status("Updating index..."):
            # Ignore type here because the meiliserach-python has the wrong type expected
            response = client.index(index).update(primaryKey=primary_key)  # type: ignore

        index_display = {
            "uid": response.uid,
            "primary_key": response.primary_key,
            "created_at": response.created_at,
            "updated_at": response.updated_at,
        }

        console.print(index_display)
    except MeiliSearchApiError as e:
        handle_index_meilisearch_api_error(e, index)


@app.command()
def update_ranking_rules(
    index: str = Argument(..., help="The name of the index for which to update the ranking rules"),
    ranking_rules: List[str] = Argument(..., help="A list of ranking rules"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Update the ranking rules of an index."""

    client_index = create_client(url, master_key).index(index)
    with console.status("Updating ranking rules..."):
        process_request(
            client_index,
            partial(client_index.update_ranking_rules, ranking_rules),
            client_index.get_ranking_rules,
            wait,
            "Update Ranking Rules",
        )


@app.command()
def update_searchable_attributes(
    index: str = Argument(
        ..., help="The name of the index for which to update the searchable attributes"
    ),
    searchable_attributes: List[str] = Argument(..., help="A list of searchable attributes"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Update the searchable attributes of an index."""

    client_index = create_client(url, master_key).index(index)
    with console.status("Updating searchable attributes..."):
        process_request(
            client_index,
            partial(client_index.update_searchable_attributes, searchable_attributes),
            client_index.get_searchable_attributes,
            wait,
            "Update Searchable Attributes",
        )


@app.command()
def update_settings(
    index: str = Argument(..., help="The name of the index to retrieve"),
    displayed_attributes: List[str] = Option(
        None, help="Fields displayed in the returned documents"
    ),
    distinct_attribute: str = Option(None, help="The distinct attribute for the index"),
    filterable_attributes: List[str] = Option(None, help="Filterable attributes for the index"),
    ranking_rules: List[str] = Option(None, help="The ranking rules for the index"),
    searchable_attributes: List[str] = Option(
        None,
        help="Fields in which to search for matching query words sorted by order of importance",
    ),
    sortable_attributes: List[str] = Option(None, help="The sortable attributes for the index"),
    stop_words: List[str] = Option(None, help="The stop words for the index"),
    synonyms: str = Option(
        None,
        help="Synonyms for the index. This should contain JSON passed as a string",
    ),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Update the settings of an index."""

    client_index = create_client(url, master_key).index(index)
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
        with console.status("Updating settings..."):
            process_request(
                client_index,
                partial(client_index.update_settings, settings),
                client_index.get_settings,
                wait,
                "Update Settings",
            )
    except json.decoder.JSONDecodeError:
        print_json_parse_error_message(synonyms)


@app.command()
def update_sortable_attributes(
    index: str = Argument(
        ..., help="The name of the index for which to update the sortable attributes"
    ),
    sortable_attributes: List[str] = Argument(..., help="A list of sortable attributes"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Update the sortable attributes of an index."""

    client_index = create_client(url, master_key).index(index)
    with console.status("Updating sortable attributes..."):
        process_request(
            client_index,
            partial(client_index.update_sortable_attributes, sortable_attributes),
            client_index.get_sortable_attributes,
            wait,
            "Update Searchable Attributes",
        )


@app.command()
def update_stop_words(
    index: str = Argument(..., help="The name of the index for which to update the stop words"),
    stop_words: List[str] = Argument(..., help="A list of stop words"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Update the stop words of an index."""

    client_index = create_client(url, master_key).index(index)
    with console.status("Updating stop words..."):
        process_request(
            client_index,
            partial(client_index.update_stop_words, stop_words),
            client_index.get_stop_words,
            wait,
            "Update Stop Words",
        )


@app.command()
def update_synonyms(
    index: str = Argument(..., help="The name of the index for which to update the synonyms"),
    synonyms: str = Argument(
        ..., help="Synonyms for the index. This should contain JSON passed as a string"
    ),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Update the searchable attributes of an index."""

    client_index = create_client(url, master_key).index(index)
    try:
        with console.status("Updating searchable attributes..."):
            process_request(
                client_index,
                partial(client_index.update_synonyms, json.loads(synonyms)),
                client_index.get_synonyms,
                wait,
                "Update Synonyms",
            )
    except json.decoder.JSONDecodeError:
        print_json_parse_error_message(synonyms)


if __name__ == "__main__":
    raise SystemExit(app())
