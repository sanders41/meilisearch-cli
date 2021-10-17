import csv
import json

import pytest
from meilisearch import Client
from typer.testing import CliRunner


@pytest.fixture(scope="session")
def index_uid():
    return "indexUID"


@pytest.fixture(scope="session")
def base_url():
    return "http://localhost:7700"


@pytest.fixture(scope="session")
def master_key():
    return "masterKey"


@pytest.fixture
def test_runner():
    return CliRunner()


@pytest.fixture
def env_vars(monkeypatch, base_url, master_key):
    monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
    monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    yield
    monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
    monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)


@pytest.fixture(scope="session")
def client(base_url, master_key):
    return Client(base_url, master_key)


@pytest.fixture
def empty_index(client, index_uid):
    def index_maker(index_name=index_uid):
        return client.create_index(uid=index_name)

    return index_maker


@pytest.fixture(autouse=True)
def clear_indexes(client):
    """
    Auto-clears the indexes after each test function run.
    Makes all the test functions independent.
    """
    # Yields back to the test function.
    yield
    # Deletes all the indexes in the MeiliSearch instance.
    indexes = client.get_indexes()
    for index in indexes:
        client.index(index.uid).delete()


@pytest.fixture(scope="session")
def small_movies():
    with open("./datasets/small_movies.json", "r", encoding="utf-8") as movie_file:
        yield json.loads(movie_file.read())


@pytest.fixture
def small_movies_json_path(small_movies, tmp_path):
    file_path = tmp_path / "small_movies.json"
    with open(file_path, "w") as f:
        json.dump(small_movies, f)

    return file_path


@pytest.fixture
def small_movies_csv_path(small_movies, tmp_path):
    file_path = tmp_path / "small_movies.csv"
    with open(file_path, "w") as f:
        field_names = list(small_movies[0].keys())
        writer = csv.DictWriter(f, fieldnames=field_names, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(small_movies)

    return file_path


@pytest.fixture
def small_movies_ndjson_path(small_movies, tmp_path):
    file_path = tmp_path / "small_movies.ndjson"
    nd_json = [json.dumps(x) for x in small_movies]
    with open(file_path, "w") as f:
        for line in nd_json:
            f.write(f"{line}\n")

    return file_path


@pytest.fixture
def index_with_documents(empty_index, small_movies, index_uid):
    def index_maker(index_name=index_uid, documents=small_movies):
        index = empty_index(index_name)
        response = index.add_documents(documents)
        index.wait_for_pending_update(response["updateId"])
        return index

    return index_maker
