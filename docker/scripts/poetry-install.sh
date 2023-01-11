#!/usr/bin/env bash

set -ex

if [[ $ENV == "prod" ]]; then
  export POETRY_INSTALL_ARGS="--no-dev"
fi

curl -sSL https://install.python-poetry.org | python3 -
poetry config virtualenvs.create false
poetry install $POETRY_INSTALL_ARGS
