import pytest

from meilisearch_cli.main import app


@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_create_dump(use_env, raw, test_runner, base_url, master_key, monkeypatch):
    args = ["dump", "create"]

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

    assert "uid" in out
    assert "status" in out

    if raw:
        assert "{" in out
        assert "}" in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_create_dump_no_url_master_key(remove_env, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["dump", "create"])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out


@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_get_dump_status(use_env, raw, test_runner, base_url, master_key, client, monkeypatch):
    response = client.create_dump()
    args = ["dump", "get-status", response["uid"]]

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

    assert "uid" in out
    assert "status" in out

    if raw:
        assert "{" in out
        assert "}" in out


@pytest.mark.parametrize("remove_env", ["all", "MEILI_HTTP_ADDR", "MEILI_MASTER_KEY"])
@pytest.mark.usefixtures("env_vars")
def test_get_dump_status_no_url_master_key(remove_env, test_runner, monkeypatch):
    if remove_env == "all":
        monkeypatch.delenv("MEILI_HTTP_ADDR", raising=False)
        monkeypatch.delenv("MEILI_MASTER_KEY", raising=False)
    else:
        monkeypatch.delenv(remove_env, raising=False)

    runner_result = test_runner.invoke(app, ["dump", "get-status", "1234-5678"])
    out = runner_result.stdout

    if remove_env == "all":
        assert "MEILI_HTTP_ADDR" in out
        assert "MEILI_MASTER_KEY" in out
    else:
        assert remove_env in out
