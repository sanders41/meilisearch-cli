from __future__ import annotations

import json
import sys
from functools import partial
from pathlib import Path
from typing import Any, List, Optional

from meilisearch import Client
from meilisearch.errors import MeiliSearchApiError
from rich.console import Console
from typer import Argument, Option, Typer

from meilisearch_cli._helpers import process_request, verify_url_and_master_key

console = Console()
app = Typer()


URL_HELP_MESSAGE = "The url to the MeiliSearch instance"
MASTER_KEY_HELP_MESSAGE = "The master key for the MeiliSearch instance"
WAIT_MESSAGE = "If this flag is set the function will wait for MeiliSearch to finish processing the data and return the results. Otherwise the update ID will be returned immediately"


@app.command()
def add_documents(
    index: str = Argument(..., help="The name of the index from which to add the documents"),
    documents: str = Argument(..., help="A JSON string of documents"),
    primary_key: str = Option(
        None,
        help="The primary key for the documents. Will be ignored if a primary key is already set",
    ),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Add documents to an index."""

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client_index = Client(url, master_key).index(index)  # type: ignore
    try:
        with console.status("Adding documents..."):
            process_request(
                client_index,
                partial(client_index.add_documents, json.loads(documents), primary_key),
                client_index.get_documents,
                wait,
                console,
            )
    except json.decoder.JSONDecodeError:
        console.print(f"Unable to parse {documents} as JSON", style="red")


@app.command()
def add_documents_from_file(
    index: str = Argument(..., help="The name of the index from which to add the documents"),
    file_path: Path = Argument(
        ...,
        exists=True,
        help="The path to the file containing the documents. Accepted file types are .json, .csv, and .ndjson",
    ),
    primary_key: str = Option(
        None,
        help="The primary key for the documents. Will be ignored if a primary key is already set",
    ),
    encoding: str = Option("utf-8", help="The encoding type for the file"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Add documents to an index from a file."""

    file_type = file_path.suffix

    if file_type not in (".json", ".csv", ".ndjson"):
        console.print(
            f"[yellow]{file_type}[/yellow] files are not accepted. Only .json, .csv, and .ndjson are accepted",
            style="red",
        )
        sys.exit()

    verify_url_and_master_key(console, url, master_key)

    with console.status("Adding documents..."):
        with open(file_path, "r") as f:
            documents = f.read().encode(encoding)

        if file_type == ".csv":
            content_type = "text/csv"
        elif file_type == ".json":
            content_type = "application/json"
        elif file_type == ".ndjson":
            content_type = "application/x-ndjson"

        # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
        # already verified they aren't None so ignore the MyPy warning
        client_index = Client(url, master_key).index(index)  # type: ignore
        process_request(
            client_index,
            partial(client_index.add_documents_raw, documents, primary_key, content_type),
            client_index.get_documents,
            wait,
            console,
        )


@app.command()
def add_documents_in_batches(
    index: str = Argument(..., help="The name of the index from which to add the documents"),
    documents: str = Argument(..., help="A JSON string of documents"),
    primary_key: str = Option(
        None,
        help="The primary key for the documents. Will be ignored if a primary key is already set",
    ),
    batch_size: int = Option(
        1000, help="The number of documents that should be included in each batch."
    ),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Add documents to an index in batches."""

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client_index = Client(url, master_key).index(index)  # type: ignore
    try:
        with console.status("Adding documents..."):
            process_request(
                client_index,
                partial(
                    client_index.add_documents_in_batches,
                    json.loads(documents),
                    batch_size,
                    primary_key,
                ),
                client_index.get_documents,
                wait,
                console,
            )
    except json.decoder.JSONDecodeError:
        console.print(f"Unable to parse {documents} as JSON", style="red")


@app.command()
def create_index(
    index: str = Argument(..., help="The name of the index to create"),
    primary_key: Optional[str] = Option(None, help="The primary key of the index"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Create an index."""

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client = Client(url, master_key)  # type: ignore
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
            console.print(f"Index [yellow]{index}[/yellow] already exists", style="red")
        else:
            raise


@app.command()
def delete_index(
    index: str = Argument(..., help="The name of the index to delete"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Delete an index."""

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client = Client(url, master_key)  # type: ignore
    try:
        with console.status("Deleting the index..."):
            response = client.index(index).delete()

        response.raise_for_status()
        console.print(f"Index {index} successfully deleted", style="green")
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index [yellow]{index}[/yellow] not found", style="red")
        else:
            raise e


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
    """Get all update update statuses of an index."""

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client = Client(url, master_key)  # type: ignore
    try:
        with console.status("Getting update status..."):
            status = client.index(index).get_all_update_status()

        console.print(status)
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index [yellow]{index}[/yellow] not found", style="red")
        else:
            raise e


@app.command()
def get_document(
    index: str = Argument(..., help="The name of the index from which to retrieve the document"),
    document_id: str = Argument(..., help="The id of the document to retrieve"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Get a document from an index."""

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client = Client(url, master_key)  # type: ignore
    try:
        with console.status("Getting document..."):
            status = client.index(index).get_document(document_id)

        console.print(status)
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index [yellow]{index}[/yellow] not found", style="red")
        else:
            raise e


@app.command()
def get_documents(
    index: str = Argument(..., help="The name of the index from which to retrieve the documents"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Get all documents from an index."""

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client = Client(url, master_key)  # type: ignore
    try:
        with console.status("Getting documents..."):
            status = client.index(index).get_documents()

        console.print(status)
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index [yellow]{index}[/yellow] not found", style="red")
        else:
            raise e


@app.command()
def get_index(
    index: str = Argument(..., help="The name of the index to retrieve"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Gets a single index."""

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client = Client(url, master_key)  # type: ignore
    try:
        with console.status("Getting index..."):
            returned_index = client.get_raw_index(index)

        console.print(returned_index)
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(e.message, style="red")
        else:
            raise e


@app.command()
def get_indexes(
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Get all indexes."""

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client = Client(url, master_key)  # type: ignore
    with console.status("Getting indexes..."):
        indexes = client.get_raw_indexes()

    console.print(indexes)


@app.command()
def get_keys(
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Gets the public and private keys"""

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client = Client(url, master_key)  # type: ignore
    with console.status("Getting keys..."):
        keys = client.get_keys()

    console.print(keys)


@app.command()
def get_primary_key(
    index: str = Argument(..., help="The name of the index from which to retrieve the primary key"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Get the primary key of an index."""

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client = Client(url, master_key)  # type: ignore
    try:
        with console.status("Getting primary key..."):
            primary_key = client.index(index).get_primary_key()

        console.print(primary_key)
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index [yellow]{index}[/yellow] not found", style="red")
        else:
            raise e


@app.command()
def get_settings(
    index: str = Argument(..., help="The name of the index from which to retrieve the settings"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Get the settings of an index."""

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client = Client(url, master_key)  # type: ignore
    try:
        with console.status("Getting settings..."):
            settings = client.index(index).get_settings()

        console.print(settings)
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index [yellow]{index}[/yellow] not found", style="red")
        else:
            raise e


@app.command()
def get_stats(
    index: str = Argument(..., help="The name of the index from which to retrieve the stats"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Get the stats of an index."""

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client = Client(url, master_key)  # type: ignore
    try:
        with console.status("Getting stats..."):
            settings = client.index(index).get_stats()

        console.print(settings)
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index [yellow]{index}[/yellow] not found", style="red")
        else:
            raise e


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

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client = Client(url, master_key)  # type: ignore
    try:
        with console.status("Getting update status..."):
            status = client.index(index).get_update_status(update_id)

        console.print(status)
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index [yellow]{index}[/yellow] not found", style="red")
        else:
            raise e


@app.command()
def get_version(
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
) -> None:
    """Gets the MeiliSearch version information."""

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client = Client(url, master_key)  # type: ignore
    with console.status("Getting version..."):
        version = client.get_version()

    console.print(version)


@app.command()
def health(
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
) -> None:
    """Checks the status of the server."""

    if not url:
        console.print(
            "A value for [yellow]--url[/yellow] has to either be provied or available in the [yellow]MEILI_HTTP_ADDR[/yellow] environment variable",
            style="red",
        )
        sys.exit()

    client = Client(url)
    with console.status("Getting server status..."):
        health = client.health()

    console.print(health)


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

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client_index = Client(url, master_key).index(index)  # type: ignore
    try:
        with console.status("Resetting displayed attributes..."):
            process_request(
                client_index,
                client_index.reset_displayed_attributes,
                client_index.get_displayed_attributes,
                wait,
                console,
            )
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index [yellow]{index}[/yellow] not found", style="red")
        else:
            raise e


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

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client_index = Client(url, master_key).index(index)  # type: ignore
    try:
        with console.status("Resetting distinct attribute..."):
            process_request(
                client_index,
                client_index.reset_distinct_attribute,
                client_index.get_distinct_attribute,
                wait,
                console,
            )
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index [yellow]{index}[/yellow] not found", style="red")
        else:
            raise e


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

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client_index = Client(url, master_key).index(index)  # type: ignore
    try:
        with console.status("Resetting filterable attributes..."):
            process_request(
                client_index,
                client_index.reset_filterable_attributes,
                client_index.get_filterable_attributes,
                wait,
                console,
            )
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index [yellow]{index}[/yellow] not found", style="red")
        else:
            raise e


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

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client_index = Client(url, master_key).index(index)  # type: ignore
    try:
        with console.status("Resetting ranking rules..."):
            process_request(
                client_index,
                client_index.reset_ranking_rules,
                client_index.get_ranking_rules,
                wait,
                console,
            )
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index [yellow]{index}[/yellow] not found", style="red")
        else:
            raise e


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

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client_index = Client(url, master_key).index(index)  # type: ignore
    try:
        with console.status("Resetting searchable attributes..."):
            process_request(
                client_index,
                client_index.reset_searchable_attributes,
                client_index.get_searchable_attributes,
                wait,
                console,
            )
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index [yellow]{index}[/yellow] not found", style="red")
        else:
            raise e


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

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client_index = Client(url, master_key).index(index)  # type: ignore
    try:
        with console.status("Resetting settings..."):
            process_request(
                client_index,
                client_index.reset_settings,
                client_index.get_settings,
                wait,
                console,
            )
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index [yellow]{index}[/yellow] not found", style="red")
        else:
            raise e


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

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client_index = Client(url, master_key).index(index)  # type: ignore
    try:
        with console.status("Resetting stop words..."):
            process_request(
                client_index,
                client_index.reset_stop_words,
                client_index.get_stop_words,
                wait,
                console,
            )
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index [yellow]{index}[/yellow] not found", style="red")
        else:
            raise e


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

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client_index = Client(url, master_key).index(index)  # type: ignore
    try:
        with console.status("Resetting synonyms..."):
            process_request(
                client_index,
                client_index.reset_synonyms,
                client_index.get_synonyms,
                wait,
                console,
            )
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index [yellow]{index}[/yellow] not found", style="red")
        else:
            raise e


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

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client = Client(url, master_key)  # type: ignore
    search_params: dict[str, Any] = {}

    if offset:
        search_params["offset"] = offset

    if limit:
        search_params["limit"] = limit

    if filter:
        search_params["filter"] = filter

    if facets_distribution:
        search_params["facetsDistribution"] = facets_distribution

    if attributes_to_retrieve:
        search_params["attributesToRetrieve"] = attributes_to_retrieve

    if attributes_to_crop:
        search_params["attributesToCrop"] = attributes_to_crop

    if crop_length:
        search_params["cropLength"] = crop_length

    if attributes_to_hightlight:
        search_params["attributesToHighlight"] = attributes_to_hightlight

    if matches:
        search_params["matches"] = matches

    if sort:
        search_params["sort"] = sort

    try:
        with console.status("Searching..."):
            if search_params:
                search_results = client.index(index).search(query, search_params)
            else:
                search_results = client.index(index).search(query)

        console.print(search_results)
    except MeiliSearchApiError as e:
        if e.error_code == "index_not_found":
            console.print(f"Index [yellow]{index}[/yellow] not found", style="red")
        else:
            raise e


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

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client_index = Client(url, master_key).index(index)  # type: ignore
    with console.status("Updating displayed attributes..."):
        process_request(
            client_index,
            partial(client_index.update_displayed_attributes, displayed_attributes),
            client_index.get_displayed_attributes,
            wait,
            console,
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

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client_index = Client(url, master_key).index(index)  # type: ignore
    with console.status("Updating distinct attribute..."):
        process_request(
            client_index,
            partial(client_index.update_distinct_attribute, distinct_attribute),
            client_index.get_distinct_attribute,
            wait,
            console,
        )


@app.command()
def update_documents(
    index: str = Argument(..., help="The name of the index from which to update the documents"),
    documents: str = Argument(..., help="A JSON string of documents"),
    primary_key: str = Option(
        None,
        help="The primary key for the documents. Will be ignored if a primary key is already set",
    ),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Update documents in an index."""

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client_index = Client(url, master_key).index(index)  # type: ignore
    try:
        with console.status("Updating documents..."):
            process_request(
                client_index,
                partial(client_index.update_documents, json.loads(documents), primary_key),
                client_index.get_documents,
                wait,
                console,
            )
    except json.decoder.JSONDecodeError:
        console.print(f"Unable to parse {documents} as JSON", style="red")


@app.command()
def update_documents_from_file(
    index: str = Argument(..., help="The name of the index from which to update the documents"),
    file_path: Path = Argument(
        ...,
        exists=True,
        help="The path to the file containing the documents. Accepted file types are .json, .csv, and .ndjson",
    ),
    primary_key: str = Option(
        None,
        help="The primary key for the documents. Will be ignored if a primary key is already set",
    ),
    encoding: str = Option("utf-8", help="The encoding type for the file"),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Update documents in an index from a file."""

    file_type = file_path.suffix

    if file_type not in (".json", ".csv", ".ndjson"):
        console.print(
            f"[yellow]{file_type}[/yellow] files are not accepted. Only .json, .csv, and .ndjson are accepted",
            style="red",
        )
        sys.exit()

    verify_url_and_master_key(console, url, master_key)

    with console.status("Adding documents..."):
        with open(file_path, "r") as f:
            documents = f.read().encode(encoding)

        if file_type == ".csv":
            content_type = "text/csv"
        elif file_type == ".json":
            content_type = "application/json"
        elif file_type == ".ndjson":
            content_type = "application/x-ndjson"

        # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
        # already verified they aren't None so ignore the MyPy warning
        client_index = Client(url, master_key).index(index)  # type: ignore
        process_request(
            client_index,
            partial(client_index.add_documents_raw, documents, primary_key, content_type),
            client_index.get_documents,
            wait,
            console,
        )


@app.command()
def update_documents_in_batches(
    index: str = Argument(..., help="The name of the index from which to add the documents"),
    documents: str = Argument(..., help="A JSON string of documents"),
    primary_key: str = Option(
        None,
        help="The primary key for the documents. Will be ignored if a primary key is already set",
    ),
    batch_size: int = Option(
        1000, help="The number of documents that should be included in each batch."
    ),
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Add documents to an index in batches."""

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client_index = Client(url, master_key).index(index)  # type: ignore
    try:
        with console.status("Adding documents..."):
            process_request(
                client_index,
                partial(
                    client_index.update_documents_in_batches,
                    json.loads(documents),
                    batch_size,
                    primary_key,
                ),
                client_index.get_documents,
                wait,
                console,
            )
    except json.decoder.JSONDecodeError:
        console.print(f"Unable to parse {documents} as JSON", style="red")


@app.command()
def update_index(
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

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client = Client(url, master_key)  # type: ignore
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
        if e.error_code == "index_not_found":
            console.print(f"Index [yellow]{index}[/yellow] not found", style="red")
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
    url: Optional[str] = Option(None, envvar="MEILI_HTTP_ADDR", help=URL_HELP_MESSAGE),
    master_key: Optional[str] = Option(
        None, envvar="MEILI_MASTER_KEY", help=MASTER_KEY_HELP_MESSAGE
    ),
    wait: bool = Option(False, "--wait", "-w", help=WAIT_MESSAGE),
) -> None:
    """Update the ranking rules of an index."""

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client_index = Client(url, master_key).index(index)  # type: ignore
    with console.status("Updating ranking rules..."):
        process_request(
            client_index,
            partial(client_index.update_ranking_rules, ranking_rules),
            client_index.get_ranking_rules,
            wait,
            console,
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

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client_index = Client(url, master_key).index(index)  # type: ignore
    with console.status("Updating searchable attributes..."):
        process_request(
            client_index,
            partial(client_index.update_searchable_attributes, searchable_attributes),
            client_index.get_searchable_attributes,
            wait,
            console,
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

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client_index = Client(url, master_key).index(index)  # type: ignore
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
                console,
            )
    except json.decoder.JSONDecodeError:
        console.print(f"Unable to parse {synonyms} as JSON", style="red")


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

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client_index = Client(url, master_key).index(index)  # type: ignore
    with console.status("Updating sortable attributes..."):
        process_request(
            client_index,
            partial(client_index.update_sortable_attributes, sortable_attributes),
            client_index.get_sortable_attributes,
            wait,
            console,
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

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client_index = Client(url, master_key).index(index)  # type: ignore
    with console.status("Updating stop words..."):
        process_request(
            client_index,
            partial(client_index.update_stop_words, stop_words),
            client_index.get_stop_words,
            wait,
            console,
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

    verify_url_and_master_key(console, url, master_key)

    # MyPy compains about optional str for url and master_key however verify_url_and_master_key has
    # already verified they aren't None so ignore the MyPy warning
    client_index = Client(url, master_key).index(index)  # type: ignore
    try:
        with console.status("Updating searchable attributes..."):
            process_request(
                client_index,
                partial(client_index.update_synonyms, json.loads(synonyms)),
                client_index.get_synonyms,
                wait,
                console,
            )
    except json.decoder.JSONDecodeError:
        console.print(f"Unable to parse {synonyms} as JSON", style="red")


if __name__ == "__main__":
    raise SystemExit(app())
