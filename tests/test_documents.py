import json
from unittest.mock import patch

import pytest
from meilisearch.errors import MeiliSearchApiError
from meilisearch.index import Index
from requests.models import Response

from meilisearch_cli.main import app
from tests.utils import get_update_id_from_output


@pytest.mark.parametrize(
    "primary_key, expected_primary_key", [(None, "id"), ("release_date", "release_date")]
)
@pytest.mark.parametrize("use_env", [True, False])
def test_add_documents_no_wait(
    use_env,
    primary_key,
    expected_primary_key,
    index_uid,
    base_url,
    master_key,
    test_runner,
    small_movies,
    monkeypatch,
    client,
):
    args = ["documents", "add", index_uid, json.dumps(small_movies)]

    if primary_key:
        args.append("--primary-key")
        args.append(primary_key)

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    runner_result = test_runner.invoke(app, args, catch_exceptions=False)
    out = runner_result.stdout

    client.index(index_uid).wait_for_task(get_update_id_from_output(out))

    assert client.index(index_uid).get_primary_key() == expected_primary_key
    assert "uid" in out


@pytest.mark.parametrize("wait_flag", ["--wait", "-w"])
@pytest.mark.parametrize("use_env", [True, False])
def test_add_documents(
    use_env,
    wait_flag,
    index_uid,
    base_url,
    master_key,
    test_runner,
    small_movies,
    monkeypatch,
):
    args = ["documents", "add", index_uid, json.dumps(small_movies), wait_flag]

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    runner_result = test_runner.invoke(app, args, catch_exceptions=False)
    out = runner_result.stdout

    assert "title" in out


def test_add_documents_no_url_master_key(index_uid, test_runner):
    runner_result = test_runner.invoke(app, ["documents", "add", index_uid, '{"test": "test"}'])
    out = runner_result.stdout

    assert "MEILI_HTTP_ADDR" in out
    assert "MEILI_MASTER_KEY" in out


@pytest.mark.usefixtures("env_vars")
def test_add_documents_json_error(
    index_uid,
    test_runner,
):
    args = ["documents", "add", index_uid, "test"]

    runner_result = test_runner.invoke(app, args, catch_exceptions=False)

    out = runner_result.stdout
    assert "Unable to parse" in out


@pytest.mark.parametrize(
    "primary_key, expected_primary_key", [(None, "id"), ("release_date", "release_date")]
)
@pytest.mark.parametrize("use_env", [True, False])
def test_add_documents_from_file_json_no_wait(
    use_env,
    primary_key,
    expected_primary_key,
    index_uid,
    base_url,
    master_key,
    test_runner,
    monkeypatch,
    client,
    small_movies_json_path,
):
    args = ["documents", "add-from-file", index_uid, str(small_movies_json_path)]

    if primary_key:
        args.append("--primary-key")
        args.append(primary_key)

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    runner_result = test_runner.invoke(app, args, catch_exceptions=False)
    out = runner_result.stdout

    client.index(index_uid).wait_for_task(get_update_id_from_output(out))

    assert client.index(index_uid).get_primary_key() == expected_primary_key
    assert "uid" in out


@pytest.mark.parametrize("wait_flag", ["--wait", "-w"])
@pytest.mark.parametrize("use_env", [True, False])
def test_add_documents_from_file_json_wait(
    use_env,
    wait_flag,
    index_uid,
    base_url,
    master_key,
    test_runner,
    monkeypatch,
    small_movies_json_path,
):
    args = ["documents", "add-from-file", index_uid, str(small_movies_json_path), wait_flag]

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    runner_result = test_runner.invoke(app, args, catch_exceptions=False)
    out = runner_result.stdout

    assert "title" in out


@pytest.mark.usefixtures("env_vars")
def test_add_documents_from_file_csv(
    index_uid,
    test_runner,
    client,
    small_movies_csv_path,
):
    args = ["documents", "add-from-file", index_uid, str(small_movies_csv_path)]

    runner_result = test_runner.invoke(app, args)
    out = runner_result.stdout

    client.index(index_uid).wait_for_task(get_update_id_from_output(out))

    assert "uid" in out


@pytest.mark.usefixtures("env_vars")
def test_add_documents_from_file_ndjson(
    index_uid,
    test_runner,
    client,
    small_movies_ndjson_path,
):
    args = ["documents", "add-from-file", index_uid, str(small_movies_ndjson_path)]

    runner_result = test_runner.invoke(app, args)
    out = runner_result.stdout

    client.index(index_uid).wait_for_task(get_update_id_from_output(out))

    assert "uid" in out


def test_add_documents_from_file_no_url_master_key(index_uid, test_runner, small_movies_json_path):
    runner_result = test_runner.invoke(
        app, ["documents", "add-from-file", index_uid, str(small_movies_json_path)]
    )
    out = runner_result.stdout

    assert "MEILI_HTTP_ADDR" in out
    assert "MEILI_MASTER_KEY" in out


@pytest.mark.usefixtures("env_vars")
def test_add_documents_from_file_bad_path(index_uid, test_runner, tmp_path):
    runner_result = test_runner.invoke(
        app,
        ["documents", "add-from-file", index_uid, str(tmp_path / "bad.json")],
    )
    out = runner_result.stdout
    assert "does not exist" in out


@pytest.mark.usefixtures("env_vars")
def test_add_documents_from_file_invalid_type(index_uid, test_runner, tmp_path):
    file_path = tmp_path / "bad.xml"
    with open(file_path, "w") as f:
        f.write("")

    runner_result = test_runner.invoke(
        app, ["documents", "add-from-file", index_uid, str(file_path)]
    )
    out = runner_result.stdout
    assert "not accepted" in out


@pytest.mark.parametrize(
    "primary_key, expected_primary_key", [(None, "id"), ("release_date", "release_date")]
)
@pytest.mark.parametrize("batch_size", [None, 10, 1000])
@pytest.mark.parametrize("use_env", [True, False])
def test_add_documents_in_batches_no_wait(
    use_env,
    primary_key,
    expected_primary_key,
    batch_size,
    index_uid,
    base_url,
    master_key,
    test_runner,
    small_movies,
    monkeypatch,
    empty_index,
):
    index = empty_index()

    args = ["documents", "add-in-batches", index_uid, json.dumps(small_movies)]

    if batch_size:
        args.append("--batch-size")
        args.append(str(batch_size))

    if primary_key:
        args.append("--primary-key")
        args.append(primary_key)

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    runner_result = test_runner.invoke(app, args, catch_exceptions=False)
    out = runner_result.stdout

    for task in index.get_tasks()["results"]:
        index.wait_for_task(task["uid"])

    assert index.get_primary_key() == expected_primary_key
    assert "uid" in out


@pytest.mark.parametrize("wait_flag", ["--wait", "-w"])
@pytest.mark.parametrize("batch_size", [None, 10, 1000])
@pytest.mark.parametrize("use_env", [True, False])
def test_add_documents_in_batches(
    use_env,
    wait_flag,
    batch_size,
    index_uid,
    base_url,
    master_key,
    test_runner,
    small_movies,
    monkeypatch,
    empty_index,
):
    index = empty_index()

    args = ["documents", "add-in-batches", index_uid, json.dumps(small_movies), wait_flag]

    if batch_size:
        args.append("--batch-size")
        args.append(str(batch_size))

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    runner_result = test_runner.invoke(app, args, catch_exceptions=False)
    out = runner_result.stdout

    for task in index.get_tasks()["results"]:
        index.wait_for_task(task["uid"])

    assert "title" in out


def test_add_documents_in_batches_no_url_master_key(index_uid, test_runner):
    runner_result = test_runner.invoke(
        app, ["documents", "add-in-batches", index_uid, '{"test": "test"}']
    )
    out = runner_result.stdout

    assert "MEILI_HTTP_ADDR" in out
    assert "MEILI_MASTER_KEY" in out


@pytest.mark.usefixtures("env_vars")
def test_add_documents_in_batches_json_error(
    index_uid,
    test_runner,
):
    args = ["documents", "add-in-batches", index_uid, "test"]

    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert "Unable to parse" in out


@pytest.mark.parametrize(
    "wait_flag, expected",
    [(None, "uid"), ("--wait", "[]"), ("-w", "[]")],
)
@pytest.mark.parametrize("use_env", [True, False])
def test_delete_all_documents(
    use_env,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    small_movies,
    monkeypatch,
    client,
):
    update = client.index(index_uid).add_documents(small_movies)
    client.index(index_uid).wait_for_task(update["uid"])

    args = ["documents", "delete-all", index_uid]

    if wait_flag:
        args.append(wait_flag)

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    runner_result = test_runner.invoke(app, args)
    out = runner_result.stdout

    if not wait_flag:
        client.index(index_uid).wait_for_task(get_update_id_from_output(out))

    assert client.index(index_uid).get_documents() == []
    assert expected in out


def test_delete_all_documents_no_url_master_key(index_uid, test_runner):
    runner_result = test_runner.invoke(app, ["documents", "delete-all", index_uid])
    out = runner_result.stdout

    assert "MEILI_HTTP_ADDR" in out
    assert "MEILI_MASTER_KEY" in out


@pytest.mark.parametrize(
    "wait_flag, expected",
    [(None, "uid"), ("--wait", "title"), ("-w", "title")],
)
@pytest.mark.parametrize("use_env", [True, False])
def test_delete_document(
    use_env,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    small_movies,
    monkeypatch,
    client,
):
    update = client.index(index_uid).add_documents(small_movies)
    client.index(index_uid).wait_for_task(update["uid"])
    documents = client.index(index_uid).get_documents()
    document_id = documents[0]["id"]

    args = ["documents", "delete", index_uid, document_id]

    if wait_flag:
        args.append(wait_flag)

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    runner_result = test_runner.invoke(app, args)
    out = runner_result.stdout

    if not wait_flag:
        client.index(index_uid).wait_for_task(get_update_id_from_output(out))

    with pytest.raises(MeiliSearchApiError):
        client.index(index_uid).get_document(document_id)

    assert expected in out


def test_delete_document_no_url_master_key(index_uid, test_runner):
    runner_result = test_runner.invoke(app, ["documents", "delete", index_uid, "1"])
    out = runner_result.stdout

    assert "MEILI_HTTP_ADDR" in out
    assert "MEILI_MASTER_KEY" in out


@pytest.mark.parametrize(
    "wait_flag, expected",
    [(None, "uid"), ("--wait", "title"), ("-w", "title")],
)
@pytest.mark.parametrize("use_env", [True, False])
def test_delete_documents(
    use_env,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    small_movies,
    monkeypatch,
    client,
):
    update = client.index(index_uid).add_documents(small_movies)
    client.index(index_uid).wait_for_task(update["uid"])
    documents = client.index(index_uid).get_documents()
    document_id_1 = documents[0]["id"]
    document_id_2 = documents[1]["id"]

    args = ["documents", "delete-multiple", index_uid, document_id_1, document_id_2]

    if wait_flag:
        args.append(wait_flag)

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    runner_result = test_runner.invoke(app, args)
    out = runner_result.stdout

    if not wait_flag:
        client.index(index_uid).wait_for_task(get_update_id_from_output(out))

    for document_id in [document_id_1, document_id_2]:
        with pytest.raises(MeiliSearchApiError):
            client.index(index_uid).get_document(document_id)

    assert expected in out


def test_delete_documents_no_url_master_key(index_uid, test_runner):
    runner_result = test_runner.invoke(app, ["documents", "delete-multiple", index_uid, "1", "2"])
    out = runner_result.stdout

    assert "MEILI_HTTP_ADDR" in out
    assert "MEILI_MASTER_KEY" in out


@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_get_all_update_status(
    use_env, raw, index_uid, base_url, master_key, test_runner, client, small_movies, monkeypatch
):
    args = ["index", "get-tasks", index_uid]

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    if raw:
        args.append("--raw")

    response = client.create_index(index_uid)
    client.wait_for_task(response["uid"])
    client_index = client.get_index(index_uid)
    client_index.add_documents(small_movies)
    client_index.add_documents(small_movies)
    runner_result = test_runner.invoke(app, args, catch_exceptions=False)

    assert len(client_index.get_tasks()["results"]) == 3

    out = runner_result.stdout
    assert "status" in out
    assert "uid" in out
    assert "type" in out
    assert "enqueuedAt" in out

    if raw:
        assert "{" in out
        assert "}" in out


def test_get_all_update_status_no_url_master_key(index_uid, test_runner):
    runner_result = test_runner.invoke(app, ["index", "get-tasks", index_uid])
    out = runner_result.stdout

    assert "MEILI_HTTP_ADDR" in out
    assert "MEILI_MASTER_KEY" in out


@pytest.mark.usefixtures("env_vars")
def test_get_all_update_status_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["index", "get-tasks", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "get_tasks")
def test_get_all_update_status_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["index", "get-tasks", index_uid], catch_exceptions=False)


@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_get_document(
    use_env, raw, index_uid, base_url, master_key, test_runner, client, small_movies, monkeypatch
):
    response = client.create_index(index_uid)
    client.wait_for_task(response["uid"])
    client_index = client.get_index(index_uid)
    update = client_index.add_documents(small_movies)
    client_index.wait_for_task(update["uid"])
    documents = client_index.get_documents()

    args = ["documents", "get", index_uid, documents[0]["id"]]

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    if raw:
        args.append("--raw")

    runner_result = test_runner.invoke(app, args, catch_exceptions=False)
    out = runner_result.stdout
    assert "title" in out
    assert "Pet Sematary" in out

    if raw:
        assert "{" in out
        assert "}" in out


def test_get_document_no_url_master_key(index_uid, test_runner):
    runner_result = test_runner.invoke(app, ["documents", "get", index_uid, "test"])
    out = runner_result.stdout

    assert "MEILI_HTTP_ADDR" in out
    assert "MEILI_MASTER_KEY" in out


@pytest.mark.usefixtures("env_vars")
def test_get_document_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["documents", "get", index_uid, "test"])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "get_document")
def test_get_document_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["documents", "get", index_uid, "test"], catch_exceptions=False)


@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_get_documents(
    use_env, raw, index_uid, base_url, master_key, test_runner, client, small_movies, monkeypatch
):
    response = client.create_index(index_uid)
    client.wait_for_task(response["uid"])
    client_index = client.get_index(index_uid)
    update = client_index.add_documents(small_movies)
    client_index.wait_for_task(update["uid"])

    args = ["documents", "get-all", index_uid]

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    if raw:
        args.append("--raw")

    runner_result = test_runner.invoke(app, args, catch_exceptions=False)
    out = runner_result.stdout

    assert "title" in out
    assert "Pet Sematary" in out
    assert "title" in out
    assert "Us" in out

    if raw:
        assert "{" in out
        assert "}" in out


def test_get_documents_no_url_master_key(index_uid, test_runner):
    runner_result = test_runner.invoke(app, ["documents", "get-all", index_uid])
    out = runner_result.stdout

    assert "MEILI_HTTP_ADDR" in out
    assert "MEILI_MASTER_KEY" in out


@pytest.mark.usefixtures("env_vars")
def test_get_documents_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["documents", "get-all", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "get_documents")
def test_get_documents_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["documents", "get-all", index_uid], catch_exceptions=False)


@pytest.mark.parametrize("raw", [True, False])
@pytest.mark.parametrize("use_env", [True, False])
def test_update_documents_no_wait(
    use_env,
    raw,
    index_uid,
    base_url,
    master_key,
    test_runner,
    small_movies,
    monkeypatch,
    client,
):
    args = ["documents", "update", index_uid, json.dumps(small_movies)]

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    if raw:
        args.append("--raw")

    update = client.index(index_uid).add_documents(small_movies)
    client.index(index_uid).wait_for_task(update["uid"])
    documents = client.index(index_uid).get_documents()
    documents[0]["title"] = "some title"
    update = client.index(index_uid).update_documents([documents[0]])
    client.index(index_uid).wait_for_task(update["uid"])

    runner_result = test_runner.invoke(app, args)
    out = runner_result.stdout

    client.index(index_uid).wait_for_task(get_update_id_from_output(out))

    assert "uid" in out

    if raw:
        assert "{" in out
        assert "}" in out


@pytest.mark.parametrize("wait_flag", ["--wait", "-w"])
@pytest.mark.usefixtures("env_vars")
def test_update_documents_wait(
    wait_flag,
    index_uid,
    test_runner,
    small_movies,
    client,
):
    updated_title = "some title"
    args = ["documents", "update", index_uid, json.dumps(small_movies), wait_flag]

    update = client.index(index_uid).add_documents(small_movies)
    client.index(index_uid).wait_for_task(update["uid"])
    documents = client.index(index_uid).get_documents()
    documents[0]["title"] = updated_title
    update = client.index(index_uid).update_documents([documents[0]])
    client.index(index_uid).wait_for_task(update["uid"])

    runner_result = test_runner.invoke(app, args)
    out = runner_result.stdout

    assert updated_title not in out


def test_update_documents_no_url_master_key(index_uid, test_runner):
    runner_result = test_runner.invoke(app, ["documents", "update", index_uid, '{"test": "test"}'])
    out = runner_result.stdout

    assert "MEILI_HTTP_ADDR" in out
    assert "MEILI_MASTER_KEY" in out


@pytest.mark.usefixtures("env_vars")
def test_update_documents_json_error(
    index_uid,
    test_runner,
):
    args = ["documents", "update", index_uid, "test"]

    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert "Unable to parse" in out


@pytest.mark.parametrize("raw", [True, False])
@pytest.mark.parametrize("use_env", [True, False])
def test_update_documents_from_file_json_no_wait(
    use_env,
    raw,
    index_uid,
    base_url,
    master_key,
    test_runner,
    monkeypatch,
    client,
    small_movies_json_path,
):
    args = ["documents", "update-from-file", index_uid, str(small_movies_json_path)]

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    if raw:
        args.append("--raw")

    runner_result = test_runner.invoke(app, args)
    out = runner_result.stdout

    client.index(index_uid).wait_for_task(get_update_id_from_output(out))

    assert "uid" in out

    if raw:
        assert "{" in out
        assert "}" in out


@pytest.mark.parametrize("wait_flag", ["--wait", "-w"])
@pytest.mark.usefixtures("env_vars")
def test_update_documents_from_file_json_wait(
    wait_flag,
    index_uid,
    test_runner,
    small_movies_json_path,
):
    args = ["documents", "update-from-file", index_uid, str(small_movies_json_path), wait_flag]

    runner_result = test_runner.invoke(app, args)
    out = runner_result.stdout

    assert "title" in out


@pytest.mark.usefixtures("env_vars")
def test_update_documents_from_file_csv(
    index_uid,
    test_runner,
    client,
    small_movies_csv_path,
):
    args = ["documents", "update-from-file", index_uid, str(small_movies_csv_path)]

    runner_result = test_runner.invoke(app, args)
    out = runner_result.stdout

    client.index(index_uid).wait_for_task(get_update_id_from_output(out))

    assert "uid" in out


@pytest.mark.usefixtures("env_vars")
def test_update_documents_from_file_ndjson(
    index_uid,
    test_runner,
    client,
    small_movies_ndjson_path,
):
    args = ["documents", "update-from-file", index_uid, str(small_movies_ndjson_path)]

    runner_result = test_runner.invoke(app, args)
    out = runner_result.stdout

    client.index(index_uid).wait_for_task(get_update_id_from_output(out))

    assert "uid" in out


def test_update_documents_from_file_no_url_master_key(
    index_uid, test_runner, small_movies_json_path
):
    runner_result = test_runner.invoke(
        app, ["documents", "update-from-file", index_uid, str(small_movies_json_path)]
    )
    out = runner_result.stdout

    assert "MEILI_HTTP_ADDR" in out
    assert "MEILI_MASTER_KEY" in out


@pytest.mark.usefixtures("env_vars")
def test_update_documents_from_file_bad_path(index_uid, test_runner, tmp_path):
    runner_result = test_runner.invoke(
        app,
        ["documents", "update-from-file", index_uid, str(tmp_path / "bad.json")],
    )
    out = runner_result.stdout
    assert "does not exist" in out


@pytest.mark.usefixtures("env_vars")
def test_update_documents_from_file_invalid_type(index_uid, test_runner, tmp_path):
    file_path = tmp_path / "bad.xml"
    with open(file_path, "w") as f:
        f.write("")

    runner_result = test_runner.invoke(
        app, ["documents", "update-from-file", index_uid, str(file_path)]
    )
    out = runner_result.stdout
    assert "not accepted" in out


@pytest.mark.parametrize("batch_size", [None, 10, 1000])
@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_update_documents_in_batches_no_wait(
    use_env,
    raw,
    batch_size,
    index_uid,
    base_url,
    master_key,
    test_runner,
    small_movies,
    monkeypatch,
    client,
):
    args = ["documents", "update-in-batches", index_uid, json.dumps(small_movies)]

    if batch_size:
        args.append("--batch-size")
        args.append(str(batch_size))

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    if raw:
        args.append("--raw")

    runner_result = test_runner.invoke(app, args, catch_exceptions=False)
    out = runner_result.stdout

    for update_id in get_update_id_from_output(out):
        client.index(index_uid).wait_for_task(update_id)

    assert "uid" in out

    if raw:
        assert "{" in out
        assert "}" in out


@pytest.mark.parametrize("wait_flag", ["--wait", "-w"])
@pytest.mark.usefixtures("env_vars")
def test_update_documents_in_batches_wait(
    wait_flag,
    index_uid,
    test_runner,
    small_movies,
):
    args = ["documents", "update-in-batches", index_uid, json.dumps(small_movies), wait_flag]

    runner_result = test_runner.invoke(app, args, catch_exceptions=False)
    out = runner_result.stdout

    assert "title" in out


def test_update_documents_in_batches_no_url_master_key(index_uid, test_runner):
    runner_result = test_runner.invoke(
        app, ["documents", "update-in-batches", index_uid, '{"test": "test"}']
    )
    out = runner_result.stdout

    assert "MEILI_HTTP_ADDR" in out
    assert "MEILI_MASTER_KEY" in out


@pytest.mark.usefixtures("env_vars")
def test_update_documents_in_batches_json_error(
    index_uid,
    test_runner,
):
    args = ["documents", "update-in-batches", index_uid, "test"]

    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert "Unable to parse" in out
