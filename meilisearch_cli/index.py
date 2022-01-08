from __future__ import annotations

import json
import sys
from functools import partial
from typing import Any, List, Optional

from meilisearch.errors import MeiliSearchApiError
from rich.traceback import install
from typer import Argument, Option, Typer

from meilisearch_cli._config import MASTER_KEY_OPTION, RAW_OPTION, URL_OPTION, WAIT_OPTION, console
from meilisearch_cli._helpers import (
    check_index_status,
    create_client,
    create_panel,
    handle_meilisearch_api_error,
    print_json_parse_error_message,
    print_panel_or_raw,
    process_request,
)

install()
app = Typer()


@app.command()
def create(
    index: str = Argument(..., help="The name of the index to create"),
    primary_key: Optional[str] = Option(None, help="The primary key of the index"),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    raw: bool = RAW_OPTION,
) -> None:
    """Create an index."""

    client = create_client(url, master_key)
    try:
        with console.status("Creating index..."):
            if primary_key:
                response = client.create_index(index, {"primaryKey": primary_key})
            else:
                response = client.create_index(index)

            client.wait_for_task(response["uid"])
            check_index_status(client.config, index, response["uid"])

        client_index = client.get_index(index)
        index_dict = client_index.__dict__
        del index_dict["config"]
        del index_dict["http"]
        print_panel_or_raw(raw, index_dict, "Index")
    except MeiliSearchApiError as e:
        handle_meilisearch_api_error(e, index)


@app.command()
def delete(
    index: str = Argument(..., help="The name of the index to delete"),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
) -> None:
    """Delete an index."""

    client = create_client(url, master_key)
    try:
        with console.status("Deleting the index..."):
            response = client.index(index).delete()
            check_index_status(client.config, index, response["uid"])

        console.print(
            create_panel(
                f"Index {index} successfully deleted",
                title="Delete Index",
            )
        )
    except MeiliSearchApiError as e:
        handle_meilisearch_api_error(e, index)


@app.command()
def get(
    index: str = Argument(..., help="The name of the index to retrieve"),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    raw: bool = RAW_OPTION,
) -> None:
    """Gets a single index."""

    client = create_client(url, master_key)
    try:
        with console.status("Getting index..."):
            returned_index = client.get_raw_index(index)
            print_panel_or_raw(raw, returned_index, "Index")
    except MeiliSearchApiError as e:
        handle_meilisearch_api_error(e, index)


@app.command()
def get_all(
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    raw: bool = RAW_OPTION,
) -> None:
    """Get all indexes."""

    client = create_client(url, master_key)
    with console.status("Getting indexes..."):
        indexes = client.get_raw_indexes()
        print_panel_or_raw(raw, indexes, "All Indexes")


@app.command()
def get_primary_key(
    index: str = Argument(..., help="The name of the index from which to retrieve the primary key"),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
) -> None:
    """Get the primary key of an index."""

    client = create_client(url, master_key)
    try:
        with console.status("Getting primary key..."):
            primary_key = client.index(index).get_primary_key()
            panel = create_panel(primary_key, title="Primary Key")
            console.print(panel)
    except MeiliSearchApiError as e:
        handle_meilisearch_api_error(e, index)


@app.command()
def get_stats(
    index: str = Argument(..., help="The name of the index from which to retrieve the stats"),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    raw: bool = RAW_OPTION,
) -> None:
    """Get the stats of an index."""

    client = create_client(url, master_key)
    try:
        with console.status("Getting stats..."):
            settings = client.index(index).get_stats()
            print_panel_or_raw(raw, settings, "Stats")
    except MeiliSearchApiError as e:
        handle_meilisearch_api_error(e, index)


@app.command()
def get_tasks(
    index: str = Argument(
        ..., help="The name of the index from which to retrieve the update status"
    ),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    raw: bool = RAW_OPTION,
) -> None:
    """Get all update statuses of an index."""

    client = create_client(url, master_key)
    try:
        with console.status("Getting update status..."):
            status = client.index(index).get_tasks()
            print_panel_or_raw(raw, status, "Update Status")
    except MeiliSearchApiError as e:
        handle_meilisearch_api_error(e, index)


@app.command()
def get_settings(
    index: str = Argument(..., help="The name of the index from which to retrieve the settings"),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    raw: bool = RAW_OPTION,
) -> None:
    """Get the settings of an index."""

    client = create_client(url, master_key)
    try:
        with console.status("Getting settings..."):
            settings = client.index(index).get_settings()
            print_panel_or_raw(raw, settings, "Settings")
    except MeiliSearchApiError as e:
        handle_meilisearch_api_error(e, index)


@app.command()
def get_task(
    index: str = Argument(
        ..., help="The name of the index from which to retrieve the update status"
    ),
    update_id: int = Argument(..., help="The update ID for the update status"),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    raw: bool = RAW_OPTION,
) -> None:
    """Get the update status of an index."""

    client = create_client(url, master_key)
    try:
        with console.status("Getting update status..."):
            status = client.index(index).get_task(update_id)
            print_panel_or_raw(raw, status, "Update Status")
    except MeiliSearchApiError as e:
        handle_meilisearch_api_error(e, index)


@app.command()
def reset_displayed_attributes(
    index: str = Argument(
        ..., help="The name of the index for which to reset the displayed attributes"
    ),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
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
                raw,
            )
    except MeiliSearchApiError as e:
        handle_meilisearch_api_error(e, index)


@app.command()
def reset_distinct_attribute(
    index: str = Argument(
        ..., help="The name of the index for which to reset the distinct attribute"
    ),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
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
                raw,
            )
    except MeiliSearchApiError as e:
        handle_meilisearch_api_error(e, index)


@app.command()
def reset_filterable_attributes(
    index: str = Argument(
        ..., help="The name of the index for which to reset the filterable attributes"
    ),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
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
                raw,
            )
    except MeiliSearchApiError as e:
        handle_meilisearch_api_error(e, index)


@app.command()
def reset_ranking_rules(
    index: str = Argument(..., help="The name of the index for which to reset the ranking rules"),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
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
                raw,
            )
    except MeiliSearchApiError as e:
        handle_meilisearch_api_error(e, index)


@app.command()
def reset_searchable_attributes(
    index: str = Argument(
        ..., help="The name of the index for which to reset the searchable attributes"
    ),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
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
                raw,
            )
    except MeiliSearchApiError as e:
        handle_meilisearch_api_error(e, index)


@app.command()
def reset_settings(
    index: str = Argument(..., help="The name of the index for which to reset the settings"),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
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
                raw,
            )
    except MeiliSearchApiError as e:
        handle_meilisearch_api_error(e, index)


@app.command()
def reset_stop_words(
    index: str = Argument(..., help="The name of the index for which to reset the stop words"),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
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
                raw,
            )
    except MeiliSearchApiError as e:
        handle_meilisearch_api_error(e, index)


@app.command()
def reset_synonyms(
    index: str = Argument(..., help="The name of the index for which to reset the synonums"),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
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
                raw,
            )
    except MeiliSearchApiError as e:
        handle_meilisearch_api_error(e, index)


@app.command()
def update_displayed_attributes(
    index: str = Argument(
        ..., help="The name of the index for which to update the diplayed attributes"
    ),
    displayed_attributes: List[str] = Argument(..., help="The displayed attributes for the index"),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
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
            raw,
        )


@app.command()
def update_distinct_attribute(
    index: str = Argument(
        ..., help="The name of the index for which to update the distinct attribute"
    ),
    distinct_attribute: str = Argument(..., help="The distinct attribute for the index"),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
) -> None:
    """Update the distinct attributes of an index."""

    client_index = create_client(url, master_key).index(index)
    with console.status("Updating distinct attribute..."):
        process_request(
            client_index,
            partial(client_index.update_distinct_attribute, distinct_attribute),  # type: ignore
            client_index.get_distinct_attribute,
            wait,
            "Update Distinct Attribute",
            raw,
        )


@app.command()
def update(
    index: str = Argument(
        ..., help="The name of the index for which the settings should be udpated"
    ),
    primary_key: str = Argument(..., help="The primary key of the index"),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    raw: bool = RAW_OPTION,
) -> None:
    """Update an index."""

    client = create_client(url, master_key)
    try:
        with console.status("Updating index..."):
            update_response = client.index(index).update(primary_key=primary_key)
            status = client.wait_for_task(update_response["uid"])
            if status["status"] == "failed":
                console.print(status)
                sys.exit(1)
            response = client.get_index(index).__dict__

        index_display = {
            "uid": response["uid"],
            "primary_key": response["primary_key"],
            "created_at": str(response["created_at"]),
            "updated_at": str(response["updated_at"]),
        }

        if raw:
            console.print_json(json.dumps(index_display))
        else:
            console.print(index_display)
    except MeiliSearchApiError as e:
        handle_meilisearch_api_error(e, index)


@app.command()
def update_ranking_rules(
    index: str = Argument(..., help="The name of the index for which to update the ranking rules"),
    ranking_rules: List[str] = Argument(..., help="A list of ranking rules"),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
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
            raw,
        )


@app.command()
def update_searchable_attributes(
    index: str = Argument(
        ..., help="The name of the index for which to update the searchable attributes"
    ),
    searchable_attributes: List[str] = Argument(..., help="A list of searchable attributes"),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
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
            raw,
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
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
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
                raw,
            )
    except json.decoder.JSONDecodeError:
        print_json_parse_error_message(synonyms)


@app.command()
def update_sortable_attributes(
    index: str = Argument(
        ..., help="The name of the index for which to update the sortable attributes"
    ),
    sortable_attributes: List[str] = Argument(..., help="A list of sortable attributes"),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
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
            raw,
        )


@app.command()
def update_stop_words(
    index: str = Argument(..., help="The name of the index for which to update the stop words"),
    stop_words: List[str] = Argument(..., help="A list of stop words"),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
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
            raw,
        )


@app.command()
def update_synonyms(
    index: str = Argument(..., help="The name of the index for which to update the synonyms"),
    synonyms: str = Argument(
        ..., help="Synonyms for the index. This should contain JSON passed as a string"
    ),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
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
                raw,
            )
    except json.decoder.JSONDecodeError:
        print_json_parse_error_message(synonyms)


if __name__ == "__main__":
    raise SystemExit(app())
