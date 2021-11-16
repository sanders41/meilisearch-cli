from unittest.mock import patch

import pytest
from meilisearch import Client
from meilisearch.errors import MeiliSearchApiError
from meilisearch.index import Index
from requests.models import Response

from meilisearch_cli.main import app
from tests.utils import get_update_id_from_output


@pytest.mark.parametrize("primary_key", [None, "alt_id"])
@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_create_index(
    primary_key, use_env, raw, test_runner, index_uid, base_url, master_key, client, monkeypatch
):
    args = ["index", "create", index_uid]
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

    if raw:
        args.append("--raw")

    runner_result = test_runner.invoke(app, args)

    result = client.get_index(index_uid)
    assert result.uid == index_uid
    assert result.primary_key == primary_key

    out = runner_result.stdout

    assert "uid" in out
    assert index_uid in out
    assert "primary_key" in out
    if primary_key:
        assert primary_key in out
    elif raw:
        assert "null" in out
    else:
        assert "None" in out

    if raw:
        assert "{" in out
        assert "}" in out


@pytest.mark.usefixtures("env_vars")
def test_create_index_exists_error(test_runner, client, index_uid):
    client.create_index(index_uid)
    runner_result = test_runner.invoke(app, ["index", "create", index_uid])
    out = runner_result.stdout
    assert "already exists" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Client, "create_index")
def test_create_index_error(mock_create, test_runner, index_uid):
    mock_create.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(
            app, ["index", "create", index_uid, "--primary-key", "alt_id"], catch_exceptions=False
        )


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_create_index_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["index", "create", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.parametrize("use_env", [True, False])
def test_delete_index(use_env, base_url, master_key, test_runner, index_uid, monkeypatch, client):
    args = ["index", "delete", index_uid]

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
    runner_result = test_runner.invoke(app, ["index", "delete", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "delete")
def test_delete_index_error(mock_delete, test_runner, index_uid):
    mock_delete.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["index", "delete", index_uid], catch_exceptions=False)


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_delete_index_no_url_maseter_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["index", "delete", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_get_index(use_env, raw, base_url, master_key, test_runner, index_uid, monkeypatch, client):
    args = ["index", "get", index_uid]

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

    client.create_index(index_uid)
    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert "uid" in out
    assert index_uid in out

    if raw:
        assert "{" in out
        assert "}" in out


@pytest.mark.usefixtures("env_vars")
def test_get_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["index", "get", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Client, "get_raw_index")
def test_get_index_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["index", "get", index_uid], catch_exceptions=False)


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_get_index_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["index", "get", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_get_indexes(
    use_env, raw, base_url, master_key, test_runner, index_uid, monkeypatch, client
):
    args = ["index", "get-all"]

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

    client.create_index(index_uid)
    index2 = "test"
    client.create_index(index2)
    assert len(client.get_indexes()) == 2
    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert "uid" in out
    assert index_uid in out
    assert index2 in out

    if raw:
        assert "{" in out
        assert "}" in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_get_indexes_no_url_master_key(remove_env, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["index", "get-all"])
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
    args = ["index", "get-primary-key", index_uid]

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

    runner_result = test_runner.invoke(app, ["index", "get-primary-key", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_get_primary_key_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["index", "get-primary-key", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "get_primary_key")
def test_get_primary_key_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["index", "get-primary-key", index_uid], catch_exceptions=False)


@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_get_settings(
    use_env, raw, index_uid, base_url, master_key, test_runner, client, monkeypatch
):
    args = ["index", "get-settings", index_uid]

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

    if raw:
        assert "{" in out
        assert "}" in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_get_settings_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["index", "get-settings", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_get_settings_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["index", "get-settings", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "get_settings")
def test_get_settings_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["index", "get-settings", index_uid], catch_exceptions=False)


@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_get_stats(use_env, raw, index_uid, base_url, master_key, test_runner, client, monkeypatch):
    args = ["index", "get-stats", index_uid]

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

    client.create_index(index_uid)
    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert "numberOfDocuments" in out

    if raw:
        assert "{" in out
        assert "}" in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_get_stats_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["index", "get-stats", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_get_stats_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["index", "get-stats", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "get_stats")
def test_get_stats_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["index", "get-stats", index_uid], catch_exceptions=False)


@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_get_update_status(
    use_env, raw, index_uid, base_url, master_key, test_runner, client, small_movies, monkeypatch
):
    args = ["index", "get-update-status", index_uid]

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

    client_index = client.create_index(index_uid)
    update = client_index.add_documents(small_movies)
    args.append(str(update["updateId"]))
    runner_result = test_runner.invoke(app, args, catch_exceptions=False)

    out = runner_result.stdout
    assert "status" in out
    assert "updateId" in out
    assert "type" in out
    assert "enqueuedAt" in out

    if raw:
        assert "{" in out
        assert "}" in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_get_update_status_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["index", "get-update-status", index_uid, "0"])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_get_update_status_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["index", "get-update-status", index_uid, "0"])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "get_update_status")
def test_get_update_status_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(
            app, ["index", "get-update-status", index_uid, "0"], catch_exceptions=False
        )


@pytest.mark.parametrize("wait_flag, expected", [(None, "updateId"), ("--wait", "*"), ("-w", "*")])
@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_reset_displayed_attributes(
    use_env,
    raw,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    args = ["index", "reset-displayed-attributes", index_uid]

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

    if raw:
        args.append("--raw")

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

    runner_result = test_runner.invoke(app, ["index", "reset-displayed-attributes", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_reset_displayed_attributes_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["index", "reset-displayed-attributes", index_uid])
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
    runner_result = test_runner.invoke(
        app, ["index", "reset-displayed-attributes", index_uid, "-w"]
    )
    out = runner_result.stdout
    assert "'status': 'failed'" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "reset_displayed_attributes")
def test_reset_displayed_attributes_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(
            app, ["index", "reset-displayed-attributes", index_uid], catch_exceptions=False
        )


@pytest.mark.parametrize("wait_flag, expected", [(None, "updateId"), ("--wait", ""), ("-w", "")])
@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_reset_distinct_attribute(
    use_env,
    raw,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    args = ["index", "reset-distinct-attribute", index_uid]

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

    if raw:
        args.append("--raw")

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

    runner_result = test_runner.invoke(app, ["index", "reset-distinct-attribute", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_reset_distinct_attribute_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["index", "reset-distinct-attribute", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "reset_distinct_attribute")
def test_reset_distinct_attribute_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(
            app, ["index", "reset-distinct-attribute", index_uid], catch_exceptions=False
        )


@pytest.mark.parametrize(
    "wait_flag, expected", [(None, "updateId"), ("--wait", "[]"), ("-w", "[]")]
)
@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_reset_filterable_attributes(
    use_env,
    raw,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    args = ["index", "reset-filterable-attributes", index_uid]

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

    if raw:
        args.append("--raw")

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

    runner_result = test_runner.invoke(app, ["index", "reset-filterable-attributes", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_reset_filterable_attributes_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["index", "reset-filterable-attributes", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "reset_filterable_attributes")
def test_reset_filterable_attributes_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(
            app, ["index", "reset-filterable-attributes", index_uid], catch_exceptions=False
        )


@pytest.mark.parametrize(
    "wait_flag, expected",
    [
        (None, "updateId"),
        ("--wait", ["words", "typo", "proximity", "attribute", "sort", "exactness"]),
        ("-w", ["words", "typo", "proximity", "attribute", "sort", "exactness"]),
    ],
)
@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_reset_ranking_rules(
    use_env,
    raw,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    args = ["index", "reset-ranking-rules", index_uid]

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

    if raw:
        args.append("--raw")

    index = client.index(index_uid)
    update = index.update_displayed_attributes(["sort", "words"])
    index.wait_for_pending_update(update["updateId"])

    assert index.get_displayed_attributes() == ["sort", "words"]
    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    for e in expected:
        assert e in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_reset_ranking_rules_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["index", "reset-ranking-rules", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_reset_ranking_rules_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["index", "reset-ranking-rules", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "reset_ranking_rules")
def test_reset_ranking_rules_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["index", "reset-ranking-rules", index_uid], catch_exceptions=False)


@pytest.mark.parametrize("wait_flag, expected", [(None, "updateId"), ("--wait", "*"), ("-w", "*")])
@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_reset_searchable_attributes(
    use_env,
    raw,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    args = ["index", "reset-searchable-attributes", index_uid]

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

    if raw:
        args.append("--raw")

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

    runner_result = test_runner.invoke(app, ["index", "reset-searchable-attributes", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_reset_searchable_attributes_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["index", "reset-searchable-attributes", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "reset_searchable_attributes")
def test_reset_searchable_attributes_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(
            app, ["index", "reset-searchable-attributes", index_uid], catch_exceptions=False
        )


@pytest.mark.parametrize("wait_flag", [None, "--wait", "-w"])
@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_reset_settings(
    use_env, raw, wait_flag, index_uid, base_url, master_key, test_runner, client, monkeypatch
):
    args = ["index", "reset-settings", index_uid]

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

    if raw:
        args.append("--raw")

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
        assert "displayedAttributes" in out
        assert "*" in out
        assert "searchableAttributes" in out
        assert "filterableAttributes" in out
        assert "[]" in out
        assert "sortableAttributes" in out
        assert "rankingRules" in out
        assert "stopWords" in out
        assert "synonyms" in out
        assert "{}" in out
        assert "distinctAttribute" in out
        assert "words" in out
        assert "typo" in out
        assert "proximity" in out
        assert "attribute" in out
        assert "sort" in out
        assert "exactness" in out
        if raw:
            assert "null" in out
        else:
            assert "None" in out
    else:
        assert "updateId" in out

    if raw:
        assert "{" in out
        assert "}" in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_reset_settings_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["index", "reset-settings", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_reset_settings_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["index", "reset-settings", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "reset_settings")
def test_reset_settings_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["index", "reset-settings", index_uid], catch_exceptions=False)


@pytest.mark.parametrize(
    "wait_flag, expected", [(None, "updateId"), ("--wait", "[]"), ("-w", "[]")]
)
@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_reset_stop_words(
    use_env,
    raw,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    args = ["index", "reset-stop-words", index_uid]

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

    if raw:
        args.append("--raw")

    index = client.index(index_uid)
    update = index.update_stop_words(["a", "the"])
    index.wait_for_pending_update(update["updateId"])

    assert index.get_stop_words() == ["a", "the"]
    runner_result = test_runner.invoke(app, args, catch_exceptions=False)

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

    runner_result = test_runner.invoke(app, ["index", "reset-stop-words", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_reset_stop_words_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["index", "reset-stop-words", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "reset_stop_words")
def test_reset_stop_words_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["index", "reset-stop-words", index_uid], catch_exceptions=False)


@pytest.mark.parametrize(
    "wait_flag, expected", [(None, "updateId"), ("--wait", "{}"), ("-w", "{}")]
)
@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_reset_synonyms(
    use_env,
    raw,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    args = ["index", "reset-synonyms", index_uid]

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

    if raw:
        args.append("--raw")

    index = client.index(index_uid)
    update = index.update_synonyms({"logan": ["marval", "wolverine"]})
    index.wait_for_pending_update(update["updateId"])

    assert index.get_synonyms() == {"logan": ["marval", "wolverine"]}
    runner_result = test_runner.invoke(app, args, catch_exceptions=False)

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

    runner_result = test_runner.invoke(app, ["index", "reset-synonyms", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_reset_synonyms_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["index", "reset-synonyms", index_uid])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "reset_synonyms")
def test_reset_synonyms_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(app, ["index", "reset-synonyms", index_uid], catch_exceptions=False)


@pytest.mark.parametrize(
    "wait_flag, expected",
    [
        (None, "updateId"),
        ("--wait", ["genre", "title"]),
        ("-w", ["genre", "title"]),
    ],
)
@pytest.mark.parametrize("raw", [True, False])
@pytest.mark.parametrize("use_env", [True, False])
def test_update_displayed_attributes(
    use_env,
    raw,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    args = ["index", "update-displayed-attributes", index_uid, "genre", "title"]

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

    if raw:
        args.append("--raw")

    index = client.create_index(index_uid)
    runner_result = test_runner.invoke(app, args)
    out = runner_result.stdout

    if not wait_flag:
        client.index(index_uid).wait_for_pending_update(get_update_id_from_output(out))

    assert index.get_displayed_attributes() == ["genre", "title"]
    for e in expected:
        assert e in out


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

    runner_result = test_runner.invoke(
        app, ["index", "update-displayed-attributes", index_uid, "title"]
    )
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
@pytest.mark.parametrize("raw", [True, False])
def test_update_distinct_attribute(
    use_env,
    raw,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    args = ["index", "update-distinct-attribute", index_uid, "title"]

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

    if raw:
        args.append("--raw")

    index = client.create_index(index_uid)
    runner_result = test_runner.invoke(app, args)
    out = runner_result.stdout

    if not wait_flag:
        client.index(index_uid).wait_for_pending_update(get_update_id_from_output(out))

    assert index.get_distinct_attribute() == "title"
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

    runner_result = test_runner.invoke(
        app, ["index", "update-distinct-attribute", index_uid, "title"]
    )
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_update_index(
    use_env,
    raw,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    primary_key = "title"
    args = ["index", "update", index_uid, "--primary-key", primary_key]

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

    index = client.create_index(index_uid)
    assert index.primary_key is None
    runner_result = test_runner.invoke(app, args, catch_exceptions=False)
    out = runner_result.stdout

    assert index.get_primary_key() == primary_key
    assert "primary_key" in out
    assert primary_key in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_update_index_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["index", "update", index_uid])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.usefixtures("env_vars")
def test_update_index_not_found_error(test_runner, index_uid):
    runner_result = test_runner.invoke(app, ["index", "update", index_uid, "--primary-key", "test"])
    out = runner_result.stdout
    assert "not found" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "update")
def test_update_index_error(mock_get, test_runner, index_uid):
    mock_get.side_effect = MeiliSearchApiError("bad", Response())
    with pytest.raises(MeiliSearchApiError):
        test_runner.invoke(
            app, ["index", "update", index_uid, "--primary-key", "test"], catch_exceptions=False
        )


@pytest.mark.usefixtures("env_vars")
def test_update_index_primary_key_exists(
    index_uid,
    test_runner,
    client,
):
    primary_key = "title"
    args = ["index", "update", index_uid, "--primary-key", primary_key]

    index = client.create_index(index_uid, {"primaryKey": "id"})
    assert index.primary_key == "id"
    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert "cannot be reset" in out


@pytest.mark.parametrize(
    "wait_flag, expected",
    [
        (None, "updateId"),
        ("--wait", ["sort", "words"]),
        ("-w", ["sort", "words"]),
    ],
)
@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_update_ranking_rules(
    use_env,
    raw,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    args = ["index", "update-ranking-rules", index_uid, "sort", "words"]

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

    if raw:
        args.append("--raw")

    index = client.create_index(index_uid)
    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout

    if not wait_flag:
        update_id = get_update_id_from_output(out)
        index.wait_for_pending_update(update_id)

    assert index.get_ranking_rules() == ["sort", "words"]
    for e in expected:
        assert e in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_update_ranking_rules_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["index", "update-ranking-rules", index_uid, "sort"])
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
        ("--wait", ["genre", "title"]),
        ("-w", ["genre", "title"]),
    ],
)
@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_update_searchable_attributes(
    use_env,
    raw,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    args = ["index", "update-searchable-attributes", index_uid, "genre", "title"]

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

    if raw:
        args.append("--raw")

    index = client.create_index(index_uid)
    runner_result = test_runner.invoke(app, args)
    out = runner_result.stdout

    if not wait_flag:
        update_id = get_update_id_from_output(out)
        index.wait_for_pending_update(update_id)

    assert index.get_searchable_attributes() == ["genre", "title"]
    for e in expected:
        assert e in out


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

    runner_result = test_runner.invoke(
        app, ["index", "update-searchable-attributes", index_uid, "title"]
    )
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.parametrize("wait_flag", [None, "--wait", "-w"])
@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_update_settings(
    use_env,
    raw,
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
        "index",
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

    if raw:
        args.append("--raw")

    index = client.create_index(index_uid)
    runner_result = test_runner.invoke(app, args)
    out = runner_result.stdout

    if not wait_flag:
        update_id = get_update_id_from_output(out)
        index.wait_for_pending_update(update_id)

    assert index.get_settings() == updated_settings

    if wait_flag:
        assert "displayedAttributes"
        for value in updated_settings["displayedAttributes"]:
            assert value in out
        assert "searchableAttributes" in out
        for value in updated_settings["searchableAttributes"]:
            assert value in out
        assert "filterableAttributes" in out
        for value in updated_settings["filterableAttributes"]:
            assert value in out
        assert "sortableAttributes" in out
        for value in updated_settings["sortableAttributes"]:
            assert value in out
        assert "rankingRules" in out
        for value in updated_settings["rankingRules"]:
            assert value in out
        assert "stopWords" in out
        for value in updated_settings["stopWords"]:
            assert value in out
        assert "synonyms" in out
        for key in updated_settings["synonyms"]:
            assert key in out
            for value in updated_settings["synonyms"][key]:  # type: ignore
                assert value in out
        assert "distinctAttribute" in out
        assert updated_settings["distinctAttribute"] in out
    else:
        assert "updateId" in out

    if raw:
        assert "{" in out
        assert "}" in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_update_settings_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(
        app, ["index", "update-settings", index_uid, "--distinct-attribute", "title"]
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
        "index",
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
        ("--wait", ["genre", "title"]),
        ("-w", ["genre", "title"]),
    ],
)
@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_update_sortable_attributes(
    use_env,
    raw,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    args = ["index", "update-sortable-attributes", index_uid, "genre", "title"]

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

    if raw:
        args.append("--raw")

    index = client.create_index(index_uid)
    runner_result = test_runner.invoke(app, args)
    out = runner_result.stdout

    if not wait_flag:
        update_id = get_update_id_from_output(out)
        index.wait_for_pending_update(update_id)

    assert index.get_sortable_attributes() == ["genre", "title"]
    for e in expected:
        assert e in out


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

    runner_result = test_runner.invoke(
        app, ["index", "update-sortable-attributes", index_uid, "title"]
    )
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
        ("--wait", ["a", "the"]),
        ("-w", ["a", "the"]),
    ],
)
@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_update_stop_words(
    use_env,
    raw,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    args = ["index", "update-stop-words", index_uid, "a", "the"]

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

    if raw:
        args.append("--raw")

    index = client.create_index(index_uid)
    runner_result = test_runner.invoke(app, args)
    out = runner_result.stdout

    if not wait_flag:
        update_id = get_update_id_from_output(out)
        index.wait_for_pending_update(update_id)

    assert index.get_stop_words() == ["a", "the"]
    for e in expected:
        assert e in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_update_stop_words_no_url_master_key(remove_env, index_uid, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["index", "update-stop-words", index_uid, "the"])
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
        ("--wait", "logan"),
        ("-w", "logan"),
    ],
)
@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_update_synonyms(
    use_env,
    raw,
    wait_flag,
    expected,
    index_uid,
    base_url,
    master_key,
    test_runner,
    client,
    monkeypatch,
):
    args = ["index", "update-synonyms", index_uid, '{"logan": ["marvel", "wolverine"]}']

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

    if raw:
        args.append("--raw")

    index = client.create_index(index_uid)
    runner_result = test_runner.invoke(app, args, catch_exceptions=False)
    out = runner_result.stdout

    if not wait_flag:
        update_id = get_update_id_from_output(out)
        index.wait_for_pending_update(update_id)

    assert index.get_synonyms() == {"logan": ["marvel", "wolverine"]}
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
        app, ["index", "update-synonyms", index_uid, '{"logan": ["marvel", "wolverine"]}']
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
        "index",
        "update-synonyms",
        index_uid,
        "test",
    ]

    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert "Unable to parse" in out
