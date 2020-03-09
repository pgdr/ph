#!/usr/bin/env python3

from setuptools import setup

import os

__pgdr = "PG Drange <pgdr@equinor.com>"
__source = "https://github.com/pgdr/ph"
__webpage = __source
__description = "ph - the tabular data shell tool"

requirements = {
    "minimum": ["pandas"],
}


def src(x):
    root = os.path.dirname(__file__)
    return os.path.abspath(os.path.join(root, x))


def _read_file(fname, op):
    with open(src(fname), "r") as fin:
        return op(fin.readlines())


def readme():
    try:
        return _read_file("README.md", lambda lines: "".join(lines))
    except:
        return __description


setup(
    version="0.0.4",
    name="ph",
    packages=["ph"],
    description=__description,
    long_description=readme(),
    long_description_content_type="text/markdown",
    author="PG Drange",
    author_email="pgdr@equinor.com",
    maintainer=__pgdr,
    url=__webpage,
    project_urls={
        "Bug Tracker": "{}/issues".format(__source),
        "Documentation": "{}/blob/master/README.md".format(__source),
        "Source Code": __source,
    },
    license="MIT",
    keywords="tabular data, pandas, csv, pipeline, unix, command line tool",
    install_requires=requirements["minimum"],
    entry_points={"console_scripts": ["ph = ph:main",],},
    test_suite="tests",
    extras_require=requirements,
)
