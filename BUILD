#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 VERSION" >&2
    exit 1
fi

VERSION="$1"

echo "Running tests..."
python -m pytest

PACKAGE_VERSION="$(python -c 'import ph; print(ph._get_version())')"

if [[ "$PACKAGE_VERSION" != "$VERSION" ]]; then
    echo "Version mismatch: requested ${VERSION}, package reports ${PACKAGE_VERSION}" >&2
    exit 1
fi

if git rev-parse "v${VERSION}" >/dev/null 2>&1; then
    echo "Git tag v${VERSION} already exists" >&2
    exit 1
fi

echo "Building ph ${VERSION}..."
rm -rf build dist ./*.egg-info

python -m pip install --upgrade build twine
python -m build
python -m twine check dist/*

echo "Built files:"
ls -l dist/

echo "Uploading ph ${VERSION} to PyPI..."
python -m twine upload dist/*

echo "Tagging release..."
git tag -a "v${VERSION}" -m "Release ${VERSION}"
git push origin "v${VERSION}"

echo "Released ph ${VERSION}"
