from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from meilisearch.errors import MeiliSearchApiError
from meilisearch.index import Index
from requests.models import Response
from tomlkit import parse

from meilisearch_cli.main import __version__, app


@pytest.fixture
def test_key(client):
    key_info = {
        "description": "test",
        "actions": ["search"],
        "indexes": ["movies"],
        "expiresAt": None,
    }

    key = client.create_key(key_info)

    yield key

    try:
        client.delete_key(key["key"])
    except MeiliSearchApiError:
        pass


@pytest.fixture
def test_key_info(client):
    key_info = {"description": "test", "actions": ["search"], "indexes": ["movies"]}

    yield key_info

    try:
        keys = client.get_keys()
        key = next(x for x in keys["results"] if x["description"] == key_info["description"])
        client.delete_key(key["key"])
    except MeiliSearchApiError:
        pass


def test_versions_match():
    pyproject_file = Path().absolute() / "pyproject.toml"
    with open(pyproject_file, "r") as f:
        content = f.read()
        data = parse(content)
        pyproject_version = data["tool"]["poetry"]["version"]  # type: ignore
    assert __version__ == pyproject_version


@pytest.mark.parametrize("args", [["--version"], ["-v"]])
def test_version(args, test_runner):
    result = test_runner.invoke(app, args, catch_exceptions=False)
    out = result.stdout
    assert __version__ in out


@patch("requests.get")
def test_docs(mock_get, test_runner):
    mock_response_content = b"""
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:news="http://www.google.com/schemas/sitemap-news/0.9" xmlns:xhtml="http://www.w3.org/1999/xhtml" xmlns:mobile="http://www.google.com/schemas/sitemap-mobile/1.0" xmlns:image="http://www.google.com/schemas/sitemap-image/1.1" xmlns:video="http://www.google.com/schemas/sitemap-video/1.1">
  <url><loc>https://docs.meilisearch.com/</loc><lastmod>2021-10-06T00:00:00.000Z</lastmod><changefreq>daily</changefreq>
  </url><url><loc>https://docs.meilisearch.com/create/how_to/</loc><lastmod>2021-06-21T00:00:00.000Z</lastmod><changefreq>daily</changefreq></url>
  <url><loc>https://docs.meilisearch.com/create/how_to/aws.html</loc><lastmod>2021-10-28T00:00:00.000Z</lastmod><changefreq>daily</changefreq></url>
  <url><loc>https://docs.meilisearch.com/create/how_to/digitalocean-droplet.html</loc><lastmod>2021-10-27T00:00:00.000Z</lastmod><changefreq>daily</changefreq></url>
  <url><loc>https://docs.meilisearch.com/learn/</loc><lastmod>2021-10-05T00:00:00.000Z</lastmod><changefreq>daily</changefreq></url>
  <url><loc>https://docs.meilisearch.com/learn/advanced/</loc><lastmod>2021-10-06T00:00:00.000Z</lastmod><changefreq>daily</changefreq></url>
  <url><loc>https://docs.meilisearch.com/learn/contributing/</loc><lastmod>2021-05-04T00:00:00.000Z</lastmod><changefreq>daily</changefreq></url>
  <url><loc>https://docs.meilisearch.com/learn/contributing/another/level/down</loc><lastmod>2021-05-04T00:00:00.000Z</lastmod><changefreq>daily</changefreq></url>
</urlset>
"""
    mock_response = Response()
    mock_response.status_code = 200
    mock_response._content = mock_response_content
    mock_get.return_value = mock_response

    expected = "Meilisearch Documentation\n├── Create\n│   └── How To\n│       ├── Aws\n│       └── Digitalocean Droplet\n└── Learn\n    ├── Advanced\n    └── Contributing\n        └── Another\n            └── Level\n                └── Down\n"
    runner_result = test_runner.invoke(app, ["docs"])
    out = runner_result.stdout
    assert expected in out


def test_api_docs_link(test_runner):
    runner_result = test_runner.invoke(app, "api-docs-link")
    out = runner_result.stdout
    assert "https://docs.meilisearch.com/reference/api/" in out


def test_docs_link(test_runner):
    runner_result = test_runner.invoke(app, "docs-link")
    out = runner_result.stdout
    assert "https://docs.meilisearch.com/" in out


@pytest.mark.parametrize(
    "expires_at",
    (
        None,
        (datetime.utcnow() + timedelta(days=2)).isoformat().split(".")[0],
    ),
)
@pytest.mark.parametrize("raw", [True, False])
@pytest.mark.usefixtures("env_vars")
def test_create_key(expires_at, raw, test_key_info, test_runner):
    args = [
        "create-key",
        "--description",
        test_key_info["description"],
        "--actions",
        *test_key_info["actions"],
        "--indexes",
        *test_key_info["indexes"],
    ]

    expires_at_expected = expires_at if expires_at else "None"

    if expires_at:
        args.append("--expires-at")
        args.append(expires_at)

    if raw:
        args.append("--raw")

        if not expires_at:
            expires_at_expected = "null"

    runner_result = test_runner.invoke(app, args, catch_exceptions=False)
    out = runner_result.stdout

    assert test_key_info["description"] in out
    for action in test_key_info["actions"]:
        assert action in out
    for index in test_key_info["indexes"]:
        assert index in out

    assert expires_at_expected in out

    if raw:
        assert "}" in out
        assert "}" in out


@pytest.mark.usefixtures("env_vars")
def test_create_key_no_vals(test_runner):
    runner_result = test_runner.invoke(app, "create-key", catch_exceptions=False)
    out = runner_result.stdout

    assert "No values" in out


@pytest.mark.usefixtures("env_vars")
def test_delete_key(test_key, test_runner, client):
    args = [
        "delete-key",
        test_key["key"],
    ]

    runner_result = test_runner.invoke(app, args, catch_exceptions=False)
    out = runner_result.stdout

    assert "204" in out

    with pytest.raises(MeiliSearchApiError):
        client.get_key(test_key["key"])


@pytest.mark.usefixtures("env_vars")
def test_get_key(test_key, test_runner):
    args = ["get-key", test_key["key"]]

    runner_result = test_runner.invoke(app, args, catch_exceptions=False)
    out = runner_result.stdout

    assert test_key["key"] in out


@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_get_keys(use_env, raw, base_url, master_key, test_runner, monkeypatch):
    args = ["get-keys"]

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

    assert "search" in out
    assert "*" in out
    if raw:
        assert "{" in out
        assert "}" in out


@pytest.mark.parametrize(
    "expires_at",
    (
        None,
        (datetime.utcnow() + timedelta(days=2)).isoformat().split(".")[0],
    ),
)
@pytest.mark.parametrize("raw", [True, False])
@pytest.mark.usefixtures("env_vars")
def test_update_key(raw, expires_at, test_key, test_runner):
    update_key_info = {
        "key": test_key["key"],
        "description": "updated",
        "actions": ["*"],
        "indexes": ["*"],
        "expiresAt": (datetime.utcnow() + timedelta(days=2)).isoformat().split(".")[0],
    }

    args = [
        "update-key",
        update_key_info["key"],
        "--description",
        update_key_info["description"],
        "--actions",
        *update_key_info["actions"],
        "--indexes",
        *update_key_info["indexes"],
    ]

    expires_at_expected = expires_at if expires_at else "None"

    if expires_at:
        args.append("--expires-at")
        args.append(expires_at)

    if raw:
        args.append("--raw")

        if not expires_at:
            expires_at_expected = "null"

    runner_result = test_runner.invoke(app, args, catch_exceptions=False)
    out = runner_result.stdout

    assert update_key_info["description"] in out
    for action in update_key_info["actions"]:
        assert action in out
    for index in update_key_info["indexes"]:
        assert index in out

    assert expires_at_expected in out

    if raw:
        assert "}" in out
        assert "}" in out


@pytest.mark.usefixtures("env_vars")
def test_update_key_no_vals(test_key, test_runner):
    runner_result = test_runner.invoke(app, ["update-key", test_key["key"]], catch_exceptions=False)
    out = runner_result.stdout

    assert "No values" in out


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
@pytest.mark.parametrize("raw", [True, False])
def test_get_version(use_env, raw, base_url, master_key, test_runner, monkeypatch):
    args = ["get-version"]

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
    assert "commitSha" in out
    assert "commitDate" in out
    assert "pkgVersion" in out

    if raw:
        assert "{" in out
        assert "}" in out


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
@pytest.mark.parametrize("raw", [True, False])
def test_health(use_env, raw, base_url, master_key, test_runner, monkeypatch):
    args = ["health"]

    if use_env:
        monkeypatch.setenv("MEILI_HTTP_ADDR", base_url)
        monkeypatch.setenv("MEILI_MASTER_KEY", master_key)
    else:
        args.append("--url")
        args.append(base_url)

    if raw:
        args.append("--raw")

    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert "status" in out

    if raw:
        assert "{" in out
        assert "}" in out


def test_health_no_url(test_runner):
    runner_result = test_runner.invoke(app, ["health"])
    out = runner_result.stdout

    assert "MEILI_HTTP_ADDR" in out


@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_search_basic(
    use_env,
    raw,
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

    if raw:
        args.append("--raw")

    index = client.index(index_uid)
    update = index.add_documents(small_movies)
    index.wait_for_task(update["uid"])
    runner_result = test_runner.invoke(app, args)

    out = runner_result.stdout
    assert "166428" in out

    if raw:
        assert "{" in out
        assert "}" in out


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
    index.wait_for_task(update["uid"])
    runner_result = test_runner.invoke(app, args, catch_exceptions=False)

    out = runner_result.stdout
    assert "Hits" in out
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
