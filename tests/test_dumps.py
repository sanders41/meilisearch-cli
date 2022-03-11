import json
import re
from datetime import datetime
from time import sleep

import pytest

from meilisearch_cli.main import app


def wait_for_dump_creation(client, dump_uid, timeout_in_ms=10000, interval_in_ms=500):
    start_time = datetime.now()
    elapsed_time = 0
    while elapsed_time < timeout_in_ms:
        dump = client.get_dump_status(dump_uid)
        if dump["status"] != "in_progress":
            return
        sleep(interval_in_ms / 1000)
        time_delta = datetime.now() - start_time
        elapsed_time = round(time_delta.seconds * 1000 + time_delta.microseconds / 1000)
    raise TimeoutError


@pytest.mark.parametrize("use_env", [True, False])
@pytest.mark.parametrize("raw", [True, False])
def test_create_dump(use_env, raw, client, test_runner, base_url, master_key, monkeypatch):
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

    runner_result = test_runner.invoke(app, args, catch_exceptions=False)
    out = runner_result.stdout
    if raw:
        uid = json.loads(out)["uid"]
    else:
        uid_re = re.search(r"\d+-\d+", out)
        if uid_re:
            uid = uid_re.group()
        else:
            uid = None
    wait_for_dump_creation(client, uid)

    assert "uid" in out
    assert "status" in out

    if raw:
        assert "{" in out
        assert "}" in out


def test_create_dump_no_url_master_key(test_runner):
    runner_result = test_runner.invoke(app, ["dump", "create"])
    out = runner_result.stdout

    assert "MEILI_HTTP_ADDR" in out
    assert "MEILI_MASTER_KEY" in out


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
    if raw:
        uid = json.loads(out)["uid"]
    else:
        uid_re = re.search(r"\d+-\d+", out)
        if uid_re:
            uid = uid_re.group()
        else:
            uid = None
    wait_for_dump_creation(client, uid)

    assert "uid" in out
    assert "status" in out

    if raw:
        assert "{" in out
        assert "}" in out


def test_get_dump_status_no_url_master_key(test_runner):
    runner_result = test_runner.invoke(app, ["dump", "get-status", "1234-5678"])
    out = runner_result.stdout

    assert "MEILI_HTTP_ADDR" in out
    assert "MEILI_MASTER_KEY" in out
