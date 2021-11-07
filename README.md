# MeiliSearch CLI

[![Tests Status](https://github.com/sanders41/meilisearch-cli/workflows/Testing/badge.svg?branch=main&event=push)](https://github.com/sanders41/meilisearch-cli/actions?query=workflow%3ATesting+branch%3Amain+event%3Apush)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/sanders41/meilisearch-cli/main.svg)](https://results.pre-commit.ci/latest/github/sanders41/meilisearch-cli/main)
[![Coverage](https://codecov.io/github/sanders41/meilisearch-cli/coverage.svg?branch=main)](https://codecov.io/gh/sanders41/meilisearch-cli)
[![PyPI version](https://badge.fury.io/py/meilisearch-cli.svg)](https://badge.fury.io/py/meilisearch-cli)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/meilisearch-cli?color=5cc141)](https://github.com/sanders41/meilisearch-cli)

A CLI for working with MeiliSearch

## Installation

Installation with [pipx](https://github.com/pypa/pipx) is recommended.

```sh
pipx install meilisearch-cli
```

Alternatively MeiliSearch CLI can be installed with pip.

```sh
pip install meilisearch-cli
```

## Usage

All commands require both a url for MeiliSearch and a master key. These values can either be passed
using the flags `--url` and `--master-key`, or can be read from the environment variables
`MEILI_HTTP_ADDR` and `MEILI_MASTER_KEY`. The one exception is the `health` comman only requires the
url, and does not take a master key.

As an example, if the `MEILI_HTTP_ADDR` and `MEILI_MASTER_KEY` vairables are not set you can
retrieve the version with:

```sh
meilisearch-cli get-version --url http://localhost:7700 --master-key masterKey
```

or if the environment variables are set you can omit `--url` and `--master-key`:

```sh
meilisearch-cli get-version
```

To see a list of available commands run:

```sh
meilisearch-cli --help
```

To get information on individual commands add the `--help` flag after the command name. For example
to get information about the `add-documents` command run:

```sh
meilisearch-cli add-documents --help
```

## Example

### Get Version

![Get Version](https://github.com/sanders41/meilisearch-cli/raw/main/imgs/get-version.png)

### Get Document

![Get Document](https://github.com/sanders41/meilisearch-cli/raw/main/imgs/get-document.png)

### Documentation

The MeiliSearch documentation sections can be displayed with clickable links to each section. The
links are built based on the current state of the documentation and will automatically stay
up-to-date with the latest documentation. To follow the links command + click on a Mac or
control + click on Linux and Windows. Note that some terminals do not support clickable links.
In this case the documentation tree will be displayed but not be clickable.

![MeiliSearch Documentation](https://github.com/sanders41/meilisearch-cli/raw/main/imgs/docs.png)
