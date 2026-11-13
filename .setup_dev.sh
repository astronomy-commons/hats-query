#!/usr/bin/env bash

# Bash Unofficial strict mode (http://redsymbol.net/articles/unofficial-bash-strict-mode/)
# and (https://disconnected.systems/blog/another-bash-strict-mode/)
set -o nounset # Any uninitialized variable is an error
set -o errexit # Exit the script on the failure of any command to execute without error
set -o pipefail # Fail command pipelines on the failure of any individual step
IFS=$'\n\t' #set internal field separator to avoid iteration errors
# Trap all exits and output something helpful
trap 's=$?; echo "$0: Error on line "$LINENO": $BASH_COMMAND"; exit $s' ERR

# This script should be run by new developers to install this package in
# editable mode and configure their local environment

echo "Checking uv"
if ! command -v uv > /dev/null; then
    echo "uv is not found, see https://docs.astral.sh/uv/getting-started/installation/ to install it."
    exit 1
fi

echo "Fetching git submodules"
git submodule update --init

echo "Installing packages and developer dependencies into .venv"
uv sync --all-packages --all-groups

echo "Installing pre-commit"
uv run pre-commit install > /dev/null

#######################################################
# Include any additional configurations below this line
#######################################################
