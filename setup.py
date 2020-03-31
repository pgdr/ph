#!/usr/bin/env python3

import os
import re
from setuptools import setup


__pgdr = "PG Drange <pgdr@equinor.com>"
__source = "https://github.com/pgdr/ph"
__webpage = __source
__description = "ph - the tabular data shell tool"

_min_req = ["pandas"]
requirements = {
    "minimum": _min_req,
    "parquet": _min_req + ["pyarrow"],
    "xls": _min_req + ["xlrd"],
    "xlsw": _min_req + ["xlrd", "xlwt"],
    "plot": _min_req + ["matplotlib"],
    "data": _min_req + ["scikit-learn"],
    "math": _min_req + ["numpy"],
    "iplot": _min_req + ["cufflinks"],
}
requirements["complete"] = sorted(set(sum(requirements.values(), [])))


def _src(x):
    root = os.path.dirname(__file__)
    return os.path.abspath(os.path.join(root, x))


def _read_file(fname, op):
    with open(_src(fname), "r") as fin:
        return op(fin.readlines())


def readme():
    try:
        return _read_file("README.md", lambda lines: "".join(lines))
    except Exception:
        return __description


VERSIONFILE = "ph/_version.py"
verstrline = open(VERSIONFILE, "rt").read()
VSRE = r"^__version__ = ['\"]([^'\"]*)['\"]"
mo = re.search(VSRE, verstrline, re.M)
if mo:
    verstr = mo.group(1)
else:
    raise RuntimeError("Unable to find version string in %s." % (VERSIONFILE,))

setup(
    version=verstr,
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
    tests_require=["pytest"],
    extras_require=requirements,
)
