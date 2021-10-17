import json
from unittest.mock import patch

import pytest
from meilisearch import Client
from meilisearch.errors import MeiliSearchApiError
from meilisearch.index import Index
from requests import Response

from meilisearch_cli.main import app


def get_update_id_from_output(output):
    output_json = json.loads(output.replace("'", '"'))
    if not isinstance(output_json, list):
        return output_json["updateId"]

    return [x["updateId"] for x in output_json]


@pytest.mark.parametrize(
    "wait_flag, expected",
    [(None, "updateId"), ("--wait", "'title':"), ("-w", "'title':")],
)
@pytest.mark.parametrize(
    "primary_key, expected_primary_key", [(None, "id"), ("release_date", "release_date")]
)
@pytest.mark.parametrize("use_env", [True, False])
def test_add_documents(
    use_env,
    wait_flag,
    primary_key,
    expected_primary_key,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    small_movies,
    monkeypatch,
    client,
):
    args = ["add-documents", index_uid, json.dumps(small_movies)]

    if primary_key:
        args.append("--primary-key")
        args.append(primary_key)

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
        client.index(index_uid).wait_for_pending_update(get_update_id_from_output(out))

    assert client.index(index_uid).get_primary_key() == expected_primary_key
    assert expected in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_get_add_documents_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["add-documents", index_uid, '{"test": "test"}'])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_add_documents_json_error(
    index_uid,
    test_runner,
):
    args = ["add-documents", index_uid, "test"]

    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert "Unable to parse" in out


@pytest.mark.parametrize(
    "wait_flag, expected",
    [(None, "updateId"), ("--wait", "'title':"), ("-w", "'title':")],
)
@pytest.mark.parametrize(
    "primary_key, expected_primary_key", [(None, "id"), ("release_date", "release_date")]
)
@pytest.mark.parametrize("use_env", [True, False])
def test_add_documents_from_file_json(
    use_env,
    wait_flag,
    primary_key,
    expected_primary_key,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    monkeypatch,
    client,
    small_movies_json_path,
):
    args = ["add-documents-from-file", index_uid, str(small_movies_json_path)]

    if primary_key:
        args.append("--primary-key")
        args.append(primary_key)

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
        client.index(index_uid).wait_for_pending_update(get_update_id_from_output(out))

    assert client.index(index_uid).get_primary_key() == expected_primary_key
    assert expected in out


@pytest.mark.parametrize(
    "wait_flag, expected",
    [(None, "updateId"), ("--wait", "'title':"), ("-w", "'title':")],
)
@pytest.mark.parametrize(
    "primary_key, expected_primary_key", [(None, "id"), ("release_date", "release_date")]
)
@pytest.mark.parametrize("use_env", [True, False])
def test_add_documents_from_file_csv(
    use_env,
    wait_flag,
    primary_key,
    expected_primary_key,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    monkeypatch,
    client,
    small_movies_csv_path,
):
    args = ["add-documents-from-file", index_uid, str(small_movies_csv_path)]

    if primary_key:
        args.append("--primary-key")
        args.append(primary_key)

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
        client.index(index_uid).wait_for_pending_update(get_update_id_from_output(out))

    assert client.index(index_uid).get_primary_key() == expected_primary_key
    assert expected in out


@pytest.mark.parametrize(
    "wait_flag, expected",
    [(None, "updateId"), ("--wait", "'title':"), ("-w", "'title':")],
)
@pytest.mark.parametrize(
    "primary_key, expected_primary_key", [(None, "id"), ("release_date", "release_date")]
)
@pytest.mark.parametrize("use_env", [True, False])
def test_add_documents_from_file_ndjson(
    use_env,
    wait_flag,
    primary_key,
    expected_primary_key,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    monkeypatch,
    client,
    small_movies_ndjson_path,
):
    args = ["add-documents-from-file", index_uid, str(small_movies_ndjson_path)]

    if primary_key:
        args.append("--primary-key")
        args.append(primary_key)

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
        client.index(index_uid).wait_for_pending_update(get_update_id_from_output(out))

    assert client.index(index_uid).get_primary_key() == expected_primary_key
    assert expected in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_get_add_documents_from_file_no_url_master_key(
    remove_env, index_uid, test_runner, small_movies_json_path, monkeypatch
):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(
        app, ["add-documents-from-file", index_uid, str(small_movies_json_path)]
    )
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_get_add_documents_from_file_bad_path(index_uid, test_runner, tmp_path):
    runner_result = test_runner.invoke(
        app,
        ["add-documents-from-file", index_uid, str(tmp_path / "bad.json")],
    )
    out = runner_result.stdout
    assert "does not exist" in out


@pytest.mark.usefixtures("env_vars")
def test_get_add_documents_from_file_invalid_type(index_uid, test_runner, tmp_path):
    file_path = tmp_path / "bad.xml"
    with open(file_path, "w") as f:
        f.write("")

    runner_result = test_runner.invoke(app, ["add-documents-from-file", index_uid, str(file_path)])
    out = runner_result.stdout
    assert "not accepted" in out


@pytest.mark.parametrize(
    "wait_flag, expected",
    [(None, "updateId"), ("--wait", "'title':"), ("-w", "'title':")],
)
@pytest.mark.parametrize(
    "primary_key, expected_primary_key", [(None, "id"), ("release_date", "release_date")]
)
@pytest.mark.parametrize("batch_size", [None, 10, 1000])
@pytest.mark.parametrize("use_env", [True, False])
def test_add_documents_in_batches(
    use_env,
    wait_flag,
    primary_key,
    expected_primary_key,
    batch_size,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    small_movies,
    monkeypatch,
    client,
):
    args = ["add-documents-in-batches", index_uid, json.dumps(small_movies)]

    if batch_size:
        args.append("--batch-size")
        args.append(str(batch_size))

    if primary_key:
        args.append("--primary-key")
        args.append(primary_key)

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

    runner_result = test_runner.invoke(app, args, catch_exceptions=False)
    out = runner_result.stdout

    if not wait_flag:
        for update_id in get_update_id_from_output(out):
            client.index(index_uid).wait_for_pending_update(update_id)

    assert client.index(index_uid).get_primary_key() == expected_primary_key
    assert expected in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_get_add_documents_in_batches_no_url_master_key(
    remove_env, index_uid, test_runner, monkeypatch
):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(
        app, ["add-documents-in-batches", index_uid, '{"test": "test"}']
    )
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_add_documents_in_batches_json_error(
    index_uid,
    test_runner,
):
    args = ["add-documents-in-batches", index_uid, "test"]

    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert "Unable to parse" in out


@pytest.mark.parametrize("primary_key", [None, "alt_id"])
@pytest.mark.parametrize("use_env", [True, False])
def test_create_index(
    primary_key, use_env, test_runner, index_uid, base_url, master_key, client, monkeypatch
):
    args = ["create-index", index_uid]
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

    runner_result = test_runner.invoke(app, args)

    result = client.get_index(index_uid)
    assert result.uid == index_uid
    assert result.primary_key == primary_key

    out = runner_result.stdout

    assert f"'uid': '{index_uid}'" in out

    if primary_key:
        assert f"'primary_key': '{primary_key}'" in out
    else:
        assert "'primary_key': None" in out


@pytest.mark.usefixtures("env_vars")
def test_create_index_exists_error(test_runner, client, index_uid):
    client.create_index(index_uid)
    runner_result = test_runner.invoke(app, ["create-index", index_uid])
    out = runner_result.stdout
    assert "already exists" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Client, "create_index")
def test_create_index_error(mock_create, test_runner, index_uid):
    mock_create.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(
            app, ["create-index", index_uid, "--primary-key", "alt_id"], catch_exceptions=False
        )


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_create_index_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["create-index", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.parametrize("use_env", [True, False])
def test_delete_index(use_env, base_url, master_key, test_runner, index_uid, monkeypatch, client):
    args = ["delete-index", index_uid]

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    client.create_index(index_uid)
    assert len(client.get_indexes()) == 1
    runner_result = test_runner.invoke(app, args)

    assert client.get_indexes() == []

    out = runner_result.stdout
    assert "successfully deleted" in out


@pytest.mark.usefixtures("env_vars")
def test_delete_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["delete-index", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "delete")
def test_delete_index_error(mock_delete, test_runner, index_uid):
    mock_delete.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["delete-index", index_uid], catch_exceptions=False)


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_delete_index_no_url_maseter_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["delete-index", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.parametrize("use_env", [True, False])
def test_get_all_update_status(
    use_env, index_uid, base_url, master_key, test_runner, client, small_movies, monkeypatch
):
    args = ["get-all-update-status", index_uid]

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    client_index = client.create_index(index_uid)
    client_index.add_documents(small_movies)
    client_index.add_documents(small_movies)
    runner_result = test_runner.invoke(app, args, catch_exceptions=False)

    assert len(client_index.get_all_update_status()) == 2

    out = runner_result.stdout
    assert "status" in out
    assert "updateId" in out
    assert "type" in out
    assert "enqueuedAt" in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_get_all_update_status_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["get-all-update-status", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_get_all_update_status_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["get-all-update-status", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "get_all_update_status")
def test_get_all_update_status_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["get-all-update-status", index_uid], catch_exceptions=False)


@pytest.mark.parametrize("use_env", [True, False])
def test_get_document(
    use_env, index_uid, base_url, master_key, test_runner, client, small_movies, monkeypatch
):
    client_index = client.create_index(index_uid)
    update = client_index.add_documents(small_movies)
    client_index.wait_for_pending_update(update["updateId"])
    documents = client_index.get_documents()

    args = ["get-document", index_uid, documents[0]["id"]]

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
    assert "'title': 'Pet Sematary'" in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_get_document_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["get-document", index_uid, "test"])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_get_document_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["get-document", index_uid, "test"])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "get_document")
def test_get_document_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["get-document", index_uid, "test"], catch_exceptions=False)


@pytest.mark.parametrize("use_env", [True, False])
def test_get_documents(
    use_env, index_uid, base_url, master_key, test_runner, client, small_movies, monkeypatch
):
    client_index = client.create_index(index_uid)
    update = client_index.add_documents(small_movies)
    client_index.wait_for_pending_update(update["updateId"])

    args = ["get-documents", index_uid]

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
    assert "'title': 'Pet Sematary'" in out
    assert "'title': 'Us'" in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_get_documents_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["get-documents", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_get_documents_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["get-documents", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "get_documents")
def test_get_documents_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["get-documents", index_uid], catch_exceptions=False)


@pytest.mark.parametrize("use_env", [True, False])
def test_get_index(use_env, base_url, master_key, test_runner, index_uid, monkeypatch, client):
    args = ["get-index", index_uid]

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    client.create_index(index_uid)
    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert f"'uid': '{index_uid}'" in out


@pytest.mark.usefixtures("env_vars")
def test_get_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["get-index", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Client, "get_raw_index")
def test_get_index_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["get-index", index_uid], catch_exceptions=False)


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_get_index_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["get-index", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.parametrize("use_env", [True, False])
def test_get_indexes(use_env, base_url, master_key, test_runner, index_uid, monkeypatch, client):
    args = ["get-indexes"]

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    client.create_index(index_uid)
    index2 = "test"
    client.create_index(index2)
    assert len(client.get_indexes()) == 2
    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert f"'uid': '{index_uid}'" in out
    assert f"'uid': '{index2}'" in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_get_indexes_no_url_master_key(remove_env, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["get-indexes"])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.parametrize("use_env", [True, False])
def test_get_keys(use_env, base_url, master_key, test_runner, monkeypatch):
    args = ["get-keys"]

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
    assert "'public':" in out
    assert "'private':" in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_get_keys_no_url_master_key(remove_env, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["get-keys"])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.parametrize("use_env", [True, False])
def test_get_primary_key(
    use_env, index_uid, base_url, master_key, test_runner, client, monkeypatch
):
    args = ["get-primary-key", index_uid]

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    primary_key = "id"
    client.create_index(index_uid, {"primaryKey": primary_key})
    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert primary_key in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_get_primary_key_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["get-primary-key", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_get_primary_key_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["get-primary-key", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "get_primary_key")
def test_get_primary_key_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["get-primary-key", index_uid], catch_exceptions=False)


@pytest.mark.parametrize("use_env", [True, False])
def test_get_settings(use_env, index_uid, base_url, master_key, test_runner, client, monkeypatch):
    args = ["get-settings", index_uid]

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    client.create_index(index_uid)
    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert "displayedAttributes" in out
    assert "searchableAttributes" in out
    assert "filterableAttributes" in out
    assert "sortableAttributes" in out
    assert "rankingRules" in out
    assert "stopWords" in out
    assert "synonyms" in out
    assert "distinctAttribute" in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_get_settings_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["get-settings", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_get_settings_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["get-settings", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "get_settings")
def test_get_settings_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["get-settings", index_uid], catch_exceptions=False)


@pytest.mark.parametrize("use_env", [True, False])
def test_get_stats(use_env, index_uid, base_url, master_key, test_runner, client, monkeypatch):
    args = ["get-stats", index_uid]

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    client.create_index(index_uid)
    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert "numberOfDocuments" in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_get_stats_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["get-stats", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_get_stats_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["get-stats", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "get_stats")
def test_get_stats_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["get-stats", index_uid], catch_exceptions=False)


@pytest.mark.parametrize("use_env", [True, False])
def test_get_update_status(
    use_env, index_uid, base_url, master_key, test_runner, client, small_movies, monkeypatch
):
    args = ["get-update-status", index_uid]

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    client_index = client.create_index(index_uid)
    update = client_index.add_documents(small_movies)
    args.append(str(update["updateId"]))
    runner_result = test_runner.invoke(app, args, catch_exceptions=False)

    out = runner_result.stdout
    assert "status" in out
    assert "updateId" in out
    assert "type" in out
    assert "enqueuedAt" in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_get_update_status_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["get-update-status", index_uid, "0"])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_get_update_status_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["get-update-status", index_uid, "0"])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "get_update_status")
def test_get_update_status_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["get-update-status", index_uid, "0"], catch_exceptions=False)


@pytest.mark.parametrize("use_env", [True, False])
def test_get_version(use_env, base_url, master_key, test_runner, monkeypatch):
    args = ["get-version"]

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
    assert "'commitSha':" in out
    assert "'commitDate':" in out
    assert "'pkgVersion':" in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_get_version_no_url_master_key(remove_env, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["get-version"])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.parametrize("use_env", [True, False])
def test_health(use_env, base_url, master_key, test_runner, monkeypatch):
    args = ["health"]

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)

    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert "{'status': 'available'}" in out


def test_health_no_url(test_runner):
    runner_result = test_runner.invoke(app, ["health"])
    out = runner_result.stdout

    assert "MEILI_HTTP_ADDR" in out


@pytest.mark.parametrize(
    "wait_flag, expected", [(None, "updateId"), ("--wait", "['*']"), ("-w", "['*']")]
)
@pytest.mark.parametrize("use_env", [True, False])
def test_reset_displayed_attributes(
    use_env, wait_flag, expected, index_uid, base_url, master_key, test_runner, client, monkeypatch
):
    args = ["reset-displayed-attributes", index_uid]

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

    index = client.index(index_uid)
    update = index.update_displayed_attributes(["title", "genre"])
    index.wait_for_pending_update(update["updateId"])

    assert index.get_displayed_attributes() == ["title", "genre"]
    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert expected in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_reset_displayed_attributes_no_url_master_key(
    remove_env, index_uid, test_runner, monkeypatch
):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["reset-displayed-attributes", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_reset_displayed_attributes_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["reset-displayed-attributes", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "get_update_status")
def test_reset_displayed_attributes_failed_status(mock_get, test_runner, client, index_uid):
    mock_get.side_effect = [
        {
            "status": "failed",
            "updateId": 0,
            "type": {"name": "ResetDisplayedAttributes", "number": 0},
            "enqueuedAt": "2021-02-14T14:07:09.364505700Z",
        }
    ]

    client.create_index(index_uid)
    runner_result = test_runner.invoke(app, ["reset-displayed-attributes", index_uid, "-w"])
    out = runner_result.stdout
    assert "'status': 'failed'" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "reset_displayed_attributes")
def test_reset_displayed_attributes_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["reset-displayed-attributes", index_uid], catch_exceptions=False)


@pytest.mark.parametrize(
    "wait_flag, expected", [(None, "updateId"), ("--wait", "None"), ("-w", "None")]
)
@pytest.mark.parametrize("use_env", [True, False])
def test_reset_distinct_attribute(
    use_env, wait_flag, expected, index_uid, base_url, master_key, test_runner, client, monkeypatch
):
    args = ["reset-distinct-attribute", index_uid]

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

    index = client.index(index_uid)
    update = index.update_distinct_attribute("title")
    index.wait_for_pending_update(update["updateId"])

    assert index.get_distinct_attribute() == "title"
    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert expected in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_reset_distinct_attribute_no_url_master_key(
    remove_env, index_uid, test_runner, monkeypatch
):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["reset-distinct-attribute", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_reset_distinct_attribute_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["reset-distinct-attribute", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "reset_distinct_attribute")
def test_reset_distinct_attribute_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["reset-distinct-attribute", index_uid], catch_exceptions=False)


@pytest.mark.parametrize(
    "wait_flag, expected", [(None, "updateId"), ("--wait", "[]"), ("-w", "[]")]
)
@pytest.mark.parametrize("use_env", [True, False])
def test_reset_filterable_attributes(
    use_env, wait_flag, expected, index_uid, base_url, master_key, test_runner, client, monkeypatch
):
    args = ["reset-filterable-attributes", index_uid]

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

    index = client.index(index_uid)
    update = index.update_displayed_attributes(["title", "genre"])
    index.wait_for_pending_update(update["updateId"])

    assert index.get_displayed_attributes() == ["title", "genre"]
    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert expected in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_reset_filterable_attributes_no_url_master_key(
    remove_env, index_uid, test_runner, monkeypatch
):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["reset-filterable-attributes", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_reset_filterable_attributes_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["reset-filterable-attributes", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "reset_filterable_attributes")
def test_reset_filterable_attributes_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["reset-filterable-attributes", index_uid], catch_exceptions=False)


@pytest.mark.parametrize(
    "wait_flag, expected",
    [
        (None, "updateId"),
        ("--wait", "['words', 'typo', 'proximity', 'attribute', 'sort', 'exactness']"),
        ("-w", "['words', 'typo', 'proximity', 'attribute', 'sort', 'exactness']"),
    ],
)
@pytest.mark.parametrize("use_env", [True, False])
def test_reset_ranking_rules(
    use_env, wait_flag, expected, index_uid, base_url, master_key, test_runner, client, monkeypatch
):
    args = ["reset-ranking-rules", index_uid]

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

    index = client.index(index_uid)
    update = index.update_displayed_attributes(["sort", "words"])
    index.wait_for_pending_update(update["updateId"])

    assert index.get_displayed_attributes() == ["sort", "words"]
    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert expected in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_reset_ranking_rules_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["reset-ranking-rules", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_reset_ranking_rules_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["reset-ranking-rules", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "reset_ranking_rules")
def test_reset_ranking_rules_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["reset-ranking-rules", index_uid], catch_exceptions=False)


@pytest.mark.parametrize(
    "wait_flag, expected", [(None, "updateId"), ("--wait", "['*']"), ("-w", "['*']")]
)
@pytest.mark.parametrize("use_env", [True, False])
def test_reset_searchable_attributes(
    use_env, wait_flag, expected, index_uid, base_url, master_key, test_runner, client, monkeypatch
):
    args = ["reset-searchable-attributes", index_uid]

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

    index = client.index(index_uid)
    update = index.update_displayed_attributes(["title", "genre"])
    index.wait_for_pending_update(update["updateId"])

    assert index.get_displayed_attributes() == ["title", "genre"]
    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert expected in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_reset_searchable_attributes_no_url_master_key(
    remove_env, index_uid, test_runner, monkeypatch
):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["reset-searchable-attributes", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_reset_searchable_attributes_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["reset-searchable-attributes", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "reset_searchable_attributes")
def test_reset_searchable_attributes_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["reset-searchable-attributes", index_uid], catch_exceptions=False)


@pytest.mark.parametrize("wait_flag", [None, "--wait", "-w"])
@pytest.mark.parametrize("use_env", [True, False])
def test_reset_settings(
    use_env, wait_flag, index_uid, base_url, master_key, test_runner, client, monkeypatch
):
    args = ["reset-settings", index_uid]

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

    index = client.index(index_uid)

    updated_settings = {
        "displayedAttributes": ["genre", "title"],
        "searchableAttributes": ["genre", "title"],
        "filterableAttributes": ["genre", "title"],
        "sortableAttributes": ["genre", "title"],
        "rankingRules": ["sort", "words"],
        "stopWords": ["a", "the"],
        "synonyms": {"logan": ["marvel", "wolverine"]},
        "distinctAttribute": "title",
    }
    update = index.update_settings(updated_settings)
    index.wait_for_pending_update(update["updateId"])
    assert index.get_settings() == updated_settings

    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    if wait_flag:
        assert "'displayedAttributes': ['*']" in out
        assert "'searchableAttributes': ['*']" in out
        assert "'filterableAttributes': []" in out
        assert "'sortableAttributes': []" in out
        assert (
            "'rankingRules': [\n        'words',\n        'typo',\n        'proximity',\n        'attribute',\n        'sort',\n        'exactness'"
            in out
        )
        assert "'stopWords': []" in out
        assert "'synonyms': {}" in out
        assert "'distinctAttribute': None" in out
    else:
        assert "updateId" in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_reset_settings_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["reset-settings", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_reset_settings_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["reset-settings", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "reset_settings")
def test_reset_settings_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["reset-settings", index_uid], catch_exceptions=False)


@pytest.mark.parametrize(
    "wait_flag, expected", [(None, "updateId"), ("--wait", "[]"), ("-w", "[]")]
)
@pytest.mark.parametrize("use_env", [True, False])
def test_reset_stop_words(
    use_env, wait_flag, expected, index_uid, base_url, master_key, test_runner, client, monkeypatch
):
    args = ["reset-stop-words", index_uid]

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

    index = client.index(index_uid)
    update = index.update_stop_words(["a", "the"])
    index.wait_for_pending_update(update["updateId"])

    assert index.get_stop_words() == ["a", "the"]
    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert expected in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_reset_stop_words_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["reset-stop-words", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_reset_stop_words_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["reset-stop-words", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "reset_stop_words")
def test_reset_stop_words_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["reset-stop-words", index_uid], catch_exceptions=False)


@pytest.mark.parametrize(
    "wait_flag, expected", [(None, "updateId"), ("--wait", "{}"), ("-w", "{}")]
)
@pytest.mark.parametrize("use_env", [True, False])
def test_reset_synonyms(
    use_env, wait_flag, expected, index_uid, base_url, master_key, test_runner, client, monkeypatch
):
    args = ["reset-synonyms", index_uid]

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

    index = client.index(index_uid)
    update = index.update_synonyms({"logan": ["marval", "wolverine"]})
    index.wait_for_pending_update(update["updateId"])

    assert index.get_synonyms() == {"logan": ["marval", "wolverine"]}
    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert expected in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_reset_syonyms_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["reset-synonyms", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_reset_synonyms_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["reset-synonyms", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "reset_synonyms")
def test_reset_synonyms_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["reset-synonyms", index_uid], catch_exceptions=False)


@pytest.mark.parametrize("use_env", [True, False])
def test_search_basic(
    use_env,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    small_movies,
    monkeypatch,
):
    args = ["search", index_uid, "How to Train Your Dragon"]

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    index = client.index(index_uid)
    update = index.add_documents(small_movies)
    index.wait_for_pending_update(update["updateId"])
    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert "166428" in out


@pytest.mark.parametrize("use_env", [True, False])
def test_search_full(
    use_env,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    small_movies,
    monkeypatch,
):
    updated_settings = {
        "filterableAttributes": ["genre"],
        "sortableAttributes": ["title"],
    }

    index = client.index(index_uid)
    index.update_settings(updated_settings)

    args = [
        "search",
        index_uid,
        "",
        "--offset",
        "1",
        "--limit",
        "10",
        "--filter",
        "genre = comedy",
        "--facets-distribution",
        "genre",
        "--attributes-to-retrieve",
        "title",
        "--attributes-to-retrieve",
        "discription",
        "--attributes-to-crop",
        "description",
        "--crop-length",
        "225",
        "--attributes-to-hightlight",
        "title",
        "--matches",
        "--sort",
        "title:asc",
    ]

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    update = index.add_documents(small_movies)
    index.wait_for_pending_update(update["updateId"])
    runner_result = test_runner.invoke(app, args, catch_exceptions=False)

    out = runner_result.stdout
    assert "hits" in out
    assert "nbHits" in out
    assert "exhaustiveNbHits" in out
    assert "query" in out
    assert "limit" in out
    assert "offset" in out
    assert "processingTimeMs" in out
    assert "facetsDistribution" in out
    assert "exhaustiveFacetsCount" in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_search_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["search", index_uid, ""])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_search_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["search", index_uid, ""])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "search")
def test_search_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["search", index_uid, ""], catch_exceptions=False)


@pytest.mark.parametrize(
    "wait_flag, expected",
    [
        (None, "updateId"),
        ("--wait", "['genre', 'title']"),
        ("-w", "['genre', 'title']"),
    ],
)
@pytest.mark.parametrize("use_env", [True, False])
def test_update_displayed_attributes(
    use_env,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    args = ["update-displayed-attributes", index_uid, "genre", "title"]

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

    index = client.create_index(index_uid)
    runner_result = test_runner.invoke(app, args)
    assert index.get_displayed_attributes() == ["genre", "title"]

    out = runner_result.stdout
    assert expected in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_update_displayed_attributes_no_url_master_key(
    remove_env, index_uid, test_runner, monkeypatch
):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["update-displayed-attributes", index_uid, "title"])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.parametrize(
    "wait_flag, expected",
    [
        (None, "updateId"),
        ("--wait", "title"),
        ("-w", "title"),
    ],
)
@pytest.mark.parametrize("use_env", [True, False])
def test_update_distinct_attribute(
    use_env,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    args = ["update-distinct-attribute", index_uid, "title"]

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

    index = client.create_index(index_uid)
    runner_result = test_runner.invoke(app, args)
    assert index.get_distinct_attribute() == "title"

    out = runner_result.stdout
    assert expected in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_update_distinct_attribute_no_url_master_key(
    remove_env, index_uid, test_runner, monkeypatch
):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["update-distinct-attribute", index_uid, "title"])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.parametrize("use_env", [True, False])
def test_update_index(
    use_env,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    primary_key = "title"
    args = ["update-index", index_uid, "--primary-key", primary_key]

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)
        args.append("--master-key")
        args.append(master_key)

    index = client.create_index(index_uid)
    assert index.primary_key is None
    runner_result = test_runner.invoke(app, args)
    assert index.get_primary_key() == primary_key

    out = runner_result.stdout
    assert f"'primary_key': '{primary_key}'" in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_update_index_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["update-index", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_update_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["update-index", index_uid, "--primary-key", "test"])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "update")
def test_update_index_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(
            app, ["update-index", index_uid, "--primary-key", "test"], catch_exceptions=False
        )


@pytest.mark.usefixtures("env_vars")
def test_update_index_primary_key_exists(
    index_uid,
    test_runner,
    client,
):
    primary_key = "title"
    args = ["update-index", index_uid, "--primary-key", primary_key]

    index = client.create_index(index_uid, {"primaryKey": "id"})
    assert index.primary_key == "id"
    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert "cannot be reset" in out


@pytest.mark.parametrize(
    "wait_flag, expected",
    [
        (None, "updateId"),
        ("--wait", "['sort', 'words']"),
        ("-w", "['sort', 'words']"),
    ],
)
@pytest.mark.parametrize("use_env", [True, False])
def test_update_ranking_rules(
    use_env,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    args = ["update-ranking-rules", index_uid, "sort", "words"]

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

    index = client.create_index(index_uid)
    runner_result = test_runner.invoke(app, args)
    assert index.get_ranking_rules() == ["sort", "words"]

    out = runner_result.stdout
    assert expected in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_update_ranking_rules_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["update-ranking-rules", index_uid, "sort"])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.parametrize(
    "wait_flag, expected",
    [
        (None, "updateId"),
        ("--wait", "['genre', 'title']"),
        ("-w", "['genre', 'title']"),
    ],
)
@pytest.mark.parametrize("use_env", [True, False])
def test_update_searchable_attributes(
    use_env,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    args = ["update-searchable-attributes", index_uid, "genre", "title"]

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

    index = client.create_index(index_uid)
    runner_result = test_runner.invoke(app, args)
    assert index.get_searchable_attributes() == ["genre", "title"]

    out = runner_result.stdout
    assert expected in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_update_searchable_attributes_no_url_master_key(
    remove_env, index_uid, test_runner, monkeypatch
):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["update-searchable-attributes", index_uid, "title"])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.parametrize("wait_flag", [None, "--wait", "-w"])
@pytest.mark.parametrize("use_env", [True, False])
def test_update_settings(
    use_env,
    wait_flag,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    updated_settings = {
        "displayedAttributes": ["genre", "title"],
        "searchableAttributes": ["genre", "title"],
        "filterableAttributes": ["genre", "title"],
        "sortableAttributes": ["genre", "title"],
        "rankingRules": ["sort", "words"],
        "stopWords": ["a", "the"],
        "synonyms": {"logan": ["marvel", "wolverine"]},
        "distinctAttribute": "title",
    }

    args = [
        "update-settings",
        index_uid,
        "--distinct-attribute",
        updated_settings["distinctAttribute"],
        "--synonyms",
        '{"logan": ["marvel", "wolverine"]}',
    ]

    for attribute in updated_settings["displayedAttributes"]:
        args.append("--displayed-attributes")
        args.append(attribute)

    for attribute in updated_settings["filterableAttributes"]:
        args.append("--filterable-attributes")
        args.append(attribute)

    for rule in updated_settings["rankingRules"]:
        args.append("--ranking-rules")
        args.append(rule)

    for attribute in updated_settings["searchableAttributes"]:
        args.append("--searchable-attributes")
        args.append(attribute)

    for attribute in updated_settings["sortableAttributes"]:
        args.append("--sortable-attributes")
        args.append(attribute)

    for word in updated_settings["stopWords"]:
        args.append("--stop-words")
        args.append(word)

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

    index = client.create_index(index_uid)
    runner_result = test_runner.invoke(app, args)
    assert index.get_settings() == updated_settings

    out = runner_result.stdout
    if wait_flag:
        assert f"'displayedAttributes': {updated_settings['displayedAttributes']}" in out
        assert f"'searchableAttributes': {updated_settings['searchableAttributes']}" in out
        assert f"'filterableAttributes': {updated_settings['filterableAttributes']}" in out
        assert f"'sortableAttributes': {updated_settings['sortableAttributes']}" in out
        assert f"'rankingRules': {updated_settings['rankingRules']}" in out
        assert f"'stopWords': {updated_settings['stopWords']}" in out
        assert f"'synonyms': {updated_settings['synonyms']}" in out
        assert f"'distinctAttribute': '{updated_settings['distinctAttribute']}'" in out
    else:
        assert "updateId" in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_update_settings_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(
        app, ["update-settings", index_uid, "--distinct-attribute", "title"]
    )
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_update_settings_json_error(
    index_uid,
    test_runner,
):
    args = [
        "update-settings",
        index_uid,
        "--synonyms",
        "test",
    ]

    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert "Unable to parse" in out


@pytest.mark.parametrize(
    "wait_flag, expected",
    [
        (None, "updateId"),
        ("--wait", "['genre', 'title']"),
        ("-w", "['genre', 'title']"),
    ],
)
@pytest.mark.parametrize("use_env", [True, False])
def test_update_sortable_attributes(
    use_env,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    args = ["update-sortable-attributes", index_uid, "genre", "title"]

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

    index = client.create_index(index_uid)
    runner_result = test_runner.invoke(app, args)
    assert index.get_sortable_attributes() == ["genre", "title"]

    out = runner_result.stdout
    assert expected in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_update_sotrable_attributes_no_url_master_key(
    remove_env, index_uid, test_runner, monkeypatch
):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["update-sortable-attributes", index_uid, "title"])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.parametrize(
    "wait_flag, expected",
    [
        (None, "updateId"),
        ("--wait", "['a', 'the']"),
        ("-w", "['a', 'the']"),
    ],
)
@pytest.mark.parametrize("use_env", [True, False])
def test_update_stop_words(
    use_env,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    args = ["update-stop-words", index_uid, "a", "the"]

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

    index = client.create_index(index_uid)
    runner_result = test_runner.invoke(app, args)
    assert index.get_stop_words() == ["a", "the"]

    out = runner_result.stdout
    assert expected in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_update_stop_words_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["update-stop-words", index_uid, "the"])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.parametrize(
    "wait_flag, expected",
    [
        (None, "updateId"),
        ("--wait", "{'logan': ['marvel', 'wolverine']}"),
        ("-w", "{'logan': ['marvel', 'wolverine']}"),
    ],
)
@pytest.mark.parametrize("use_env", [True, False])
def test_update_synonyms(
    use_env,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    args = ["update-synonyms", index_uid, '{"logan": ["marvel", "wolverine"]}']

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

    index = client.create_index(index_uid)
    runner_result = test_runner.invoke(app, args)
    assert index.get_synonyms() == {"logan": ["marvel", "wolverine"]}

    out = runner_result.stdout
    assert expected in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_update_synonyms_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(
        app, ["update-synonyms", index_uid, '{"logan": ["marvel", "wolverine"]}']
    )
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_update_synonyms_json_error(
    index_uid,
    test_runner,
):
    args = [
        "update-synonyms",
        index_uid,
        "test",
    ]

    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert "Unable to parse" in out
