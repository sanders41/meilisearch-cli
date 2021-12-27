from pathlib import Path
from unittest.mock import patch

import pytest
from meilisearch.errors import MeiliSearchApiError
from meilisearch.index import Index
from requests.models import Response
from tomlkit import parse

from meilisearch_cli.main import __version__, app


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

    expected = "Meilisearch Documentation                                                       \n├── Create                                                                      \n│   └── How To                                                                  \n│       ├── Aws                                                                 \n│       └── Digitalocean Droplet                                                \n└── Learn                                                                       \n    ├── Advanced                                                                \n    └── Contributing"
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

    assert "public" in out
    assert "private" in out
    if raw:
        assert "{" in out
        assert "}" in out


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
    index.wait_for_pending_update(update["updateId"])
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
    index.wait_for_pending_update(update["updateId"])
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
