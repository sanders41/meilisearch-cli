import json
from unittest.mock import patch

import pytest
import requests
from meilisearch.errors import MeilisearchError
from meilisearch.index import Index

from meilisearch_cli._config import console
from meilisearch_cli._helpers import check_index_status, create_panel
from meilisearch_cli.main import app


@pytest.mark.parametrize("fit", [True, False])
def test_create_panel(fit, capfd):
    title = "test title"
    data = {"id": 1, "name": "test"}
    panel = create_panel(data, title=title, fit=fit)
    console.print(panel)
    out, _ = capfd.readouterr()
    assert "id: 1" in out
    assert "name: test" in out
    assert title in out


@patch("requests.get")
def test_check_index_status_error(mock_get, client):
    def mock_response(*args, **kwargs):
        response = requests.Response()
        data = {
            "status": "failed",
            "uid": 0,
            "type": {"name": "ResetDisplayedAttributes", "number": 0},
            "error": {
                "code": "none",
                "message": "test",
            },
            "enqueuedAt": "2021-02-14T14:07:09.364505700Z",
        }

        response.status_code = 200
        response._content = str.encode(json.dumps(data))

        return response

    mock_get.side_effect = mock_response

    with pytest.raises(MeilisearchError):
        check_index_status(client.config, "test", 1)


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "wait_for_task")
def test_process_request_wait_fail_single(mock_get, test_runner, index_uid, empty_index):
    empty_index()
    mock_get.side_effect = [
        {
            "status": "failed",
            "uid": 0,
            "type": {"name": "ResetDisplayedAttributes", "number": 0},
            "error": {
                "code": "index_already_exists",
            },
            "enqueuedAt": "2021-02-14T14:07:09.364505700Z",
        }
    ]

    runner_result = test_runner.invoke(
        app, ["index", "reset-displayed-attributes", index_uid, "-w"], catch_exceptions=False
    )
    out = runner_result.stdout

    assert "failed" in out


@pytest.mark.usefixtures("env_vars")
@patch.object(Index, "wait_for_task")
def test_process_request_wait_fail_multi(
    mock_get, test_runner, index_uid, empty_index, small_movies
):
    empty_index()
    mock_get.side_effect = [
        {
            "status": "failed",
            "uid": 0,
            "type": {"name": "ResetDisplayedAttributes", "number": 0},
            "error": {
                "code": "index_already_exists",
            },
            "enqueuedAt": "2021-02-14T14:07:09.364505700Z",
        }
    ]

    args = [
        "documents",
        "add-in-batches",
        index_uid,
        json.dumps(small_movies),
        "--batch-size",
        2,
        "-w",
    ]
    runner_result = test_runner.invoke(app, args, catch_exceptions=False)
    out = runner_result.stdout

    assert "failed" in out
