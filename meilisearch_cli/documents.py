from __future__ import annotations

import json
from functools import partial
from pathlib import Path
from typing import List, Optional

from meilisearch.errors import MeiliSearchApiError
from rich.traceback import install
from typer import Argument, Option, Typer

from meilisearch_cli._config import MASTER_KEY_OPTION, RAW_OPTION, URL_OPTION, WAIT_OPTION, console
from meilisearch_cli._helpers import (
    create_client,
    handle_meilisearch_api_error,
    print_json_parse_error_message,
    print_panel_or_raw,
    process_request,
    validate_file_type_and_set_content_type,
)

install()
app = Typer()


@app.command()
def add(
    index: str = Argument(..., help="The name of the index from which to add the documents"),
    documents: str = Argument(..., help="A JSON string of documents"),
    primary_key: str = Option(
        None,
        help="The primary key for the documents. Will be ignored if a primary key is already set",
    ),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
) -> None:
    """Add documents to an index."""

    client_index = create_client(url, master_key).index(index)
    try:
        with console.status("Adding documents..."):
            process_request(
                client_index,
                partial(client_index.add_documents, json.loads(documents), primary_key),
                client_index.get_documents,
                wait,
                "Add Documents Result",
                raw,
            )
    except json.decoder.JSONDecodeError:
        print_json_parse_error_message(documents)


@app.command()
def add_from_file(
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
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
) -> None:
    """Add documents to an index from a file."""

    content_type = validate_file_type_and_set_content_type(file_path)

    with console.status("Adding documents..."):
        with open(file_path, "r") as f:
            documents = f.read().encode(encoding)

        client_index = create_client(url, master_key).index(index)
        process_request(
            client_index,
            partial(client_index.add_documents_raw, documents, primary_key, content_type),
            client_index.get_documents,
            wait,
            "Add Documents Result",
            raw,
        )


@app.command()
def add_in_batches(
    index: str = Argument(..., help="The name of the index from which to add the documents"),
    documents: str = Argument(..., help="A JSON string of documents"),
    primary_key: str = Option(
        None,
        help="The primary key for the documents. Will be ignored if a primary key is already set",
    ),
    batch_size: int = Option(
        1000, help="The number of documents that should be included in each batch."
    ),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
) -> None:
    """Add documents to an index in batches."""

    client_index = create_client(url, master_key).index(index)
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
                "Add Documents Result",
                raw,
            )
    except json.decoder.JSONDecodeError:
        print_json_parse_error_message(documents)


@app.command()
def delete_all(
    index: str = Argument(..., help="The name of the index from which to delete the documents"),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
) -> None:
    """Delete all documents from an index."""

    client_index = create_client(url, master_key).index(index)
    with console.status("Deleting all documents..."):
        process_request(
            client_index,
            partial(client_index.delete_all_documents),
            client_index.get_documents,
            wait,
            "Delete Documents Result",
            raw,
        )


@app.command()
def delete(
    index: str = Argument(..., help="The name of the index from which to delete the document"),
    document_id: str = Argument(..., help="The ID for the document to delete"),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
) -> None:
    """Delete a document from an index."""

    client_index = create_client(url, master_key).index(index)
    with console.status("Deleting document..."):
        process_request(
            client_index,
            partial(client_index.delete_document, document_id),
            client_index.get_documents,
            wait,
            "Delete Document Result",
            raw,
        )


@app.command()
def delete_multiple(
    index: str = Argument(..., help="The name of the index from which to delete the documents"),
    document_ids: List[str] = Argument(..., help="The IDs for the documents to delete"),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
) -> None:
    """Delete multiple documents from an index."""

    client_index = create_client(url, master_key).index(index)
    with console.status("Deleting documents..."):
        process_request(
            client_index,
            partial(client_index.delete_documents, document_ids),
            client_index.get_documents,
            wait,
            "Delete Documents Result",
            raw,
        )


@app.command()
def get(
    index: str = Argument(..., help="The name of the index from which to retrieve the document"),
    document_id: str = Argument(..., help="The id of the document to retrieve"),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    raw: bool = RAW_OPTION,
) -> None:
    """Get a document from an index."""

    client = create_client(url, master_key)
    try:
        with console.status("Getting document..."):
            document = client.index(index).get_document(document_id)
            print_panel_or_raw(raw, document, "Document")
    except MeiliSearchApiError as e:
        handle_meilisearch_api_error(e, index)


@app.command()
def get_all(
    index: str = Argument(..., help="The name of the index from which to retrieve the documents"),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    raw: bool = RAW_OPTION,
) -> None:
    """Get all documents from an index."""
    client = create_client(url, master_key)
    try:
        with console.status("Getting documents..."):
            status = client.index(index).get_documents()
            print_panel_or_raw(raw, status, "Documents")
    except MeiliSearchApiError as e:
        handle_meilisearch_api_error(e, index)


@app.command()
def update(
    index: str = Argument(..., help="The name of the index from which to update the documents"),
    documents: str = Argument(..., help="A JSON string of documents"),
    primary_key: str = Option(
        None,
        help="The primary key for the documents. Will be ignored if a primary key is already set",
    ),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
) -> None:
    """Update documents in an index."""

    client_index = create_client(url, master_key).index(index)
    try:
        with console.status("Updating documents..."):
            process_request(
                client_index,
                partial(client_index.update_documents, json.loads(documents), primary_key),
                client_index.get_documents,
                wait,
                "Update Documents",
                raw,
            )
    except json.decoder.JSONDecodeError:
        print_json_parse_error_message(documents)


@app.command()
def update_from_file(
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
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
) -> None:
    """Update documents in an index from a file."""

    content_type = validate_file_type_and_set_content_type(file_path)

    with console.status("Adding documents..."):
        with open(file_path, "r") as f:
            documents = f.read().encode(encoding)

        client_index = create_client(url, master_key).index(index)
        process_request(
            client_index,
            partial(client_index.add_documents_raw, documents, primary_key, content_type),
            client_index.get_documents,
            wait,
            "Update Documents",
            raw,
        )


@app.command()
def update_in_batches(
    index: str = Argument(..., help="The name of the index from which to add the documents"),
    documents: str = Argument(..., help="A JSON string of documents"),
    primary_key: str = Option(
        None,
        help="The primary key for the documents. Will be ignored if a primary key is already set",
    ),
    batch_size: int = Option(
        1000, help="The number of documents that should be included in each batch."
    ),
    url: Optional[str] = URL_OPTION,
    master_key: Optional[str] = MASTER_KEY_OPTION,
    wait: bool = WAIT_OPTION,
    raw: bool = RAW_OPTION,
) -> None:
    """Add documents to an index in batches."""

    client_index = create_client(url, master_key).index(index)
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
                "Update Documents",
                raw,
            )
    except json.decoder.JSONDecodeError:
        print_json_parse_error_message(documents)


if __name__ == "__main__":
    raise SystemExit(app())
