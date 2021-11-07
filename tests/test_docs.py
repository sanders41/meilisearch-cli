import pytest

from meilisearch_cli._docs import TreeBuildError, build_tree


def test_build_tree_error():
    with pytest.raises(TreeBuildError):
        build_tree(links=[])
