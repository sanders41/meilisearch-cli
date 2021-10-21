import pytest
from rich.console import Console

from meilisearch_cli._helpers import create_panel


@pytest.mark.parametrize("fit", [True, False])
def test_create_panel(fit, capfd):
    title = "test title"
    data = {"id": 1, "name": "test"}
    panel = create_panel(data, title=title, fit=fit)
    console = Console()
    console.print(panel)
    out, _ = capfd.readouterr()
    assert "id: 1" in out
    assert "name: test" in out
    assert title in out
