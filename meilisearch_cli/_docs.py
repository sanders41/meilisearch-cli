from __future__ import annotations

import re

import requests
from bs4 import BeautifulSoup
from rich.tree import Tree


class TreeBuildError(Exception):
    pass


def build_docs_tree() -> Tree:
    links = get_links()
    return build_tree(links)


def build_branches(tree: Tree, branch: str, link: str, depth: int) -> Tree:
    def get_branch_name(node: Tree) -> str:
        return re.findall(r"(?<=\]).*(?=\[/link\])", node.label.__str__())[0]

    nodes = [tree]

    i = 0
    while nodes:
        node = nodes.pop()
        if branch == get_branch_name(node):
            return tree
        if i < depth:
            if depth == i + 1:
                branches = [get_branch_name(x) for x in node.children if node.children]
                if branch not in branches:
                    node.add(f"[link={link}]{branch}[/link]")
                    return tree
            for child in node.children:
                if child:
                    nodes.append(child)
        i += 1

    return tree  # pragma: no cover


def build_tree(links: list[str]) -> Tree:
    tree: Tree | None = None
    for link in links:
        split = split_link(link)
        for i, item in enumerate(split):
            formatted_item = format_section(item)
            if tree:
                tree = build_branches(tree, formatted_item, link, i)
            elif i == 0:
                tree = Tree(label=f"[link={link}]{formatted_item}[/link]")
    if not tree:
        raise TreeBuildError("Error building tree")

    return tree


def format_section(section: str) -> str:
    if "_" not in section and "." not in section and "-" not in section:
        return section.title()

    formatted = " ".join(section.split("_")).title() if "_" in section else section.title()
    if "-" in formatted:
        formatted = " ".join(formatted.split("-")).title()
    if "." in formatted:
        formatted = formatted.split(".")[0]
    return formatted


def get_links() -> list[str]:
    response = requests.get("https://docs.meilisearch.com/sitemap.xml")
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    return [
        x.text for x in soup.find_all("loc") if x.text != "https://docs.meilisearch.com/404.html"
    ]


def split_link(link: str) -> list[str]:
    split = link.replace("https://docs.meilisearch.com", "MeiliSearch Documentation").split("/")
    if split[-1] == "":
        split.pop()
    return split


if __name__ == "__main__":
    raise SystemExit(build_docs_tree())
