import setuptools
import os


def _here(fname):
    return os.path.join(os.path.dirname(__file__), fname)


README = ""
with open(_here("README.md"), "r") as fin:
    README = "".join(fin.readlines())

REQUIREMENTS = []
with open(_here("requirements.txt"), "r") as fin:
    REQUIREMENTS = [x.strip() for x in fin.readlines()]


setuptools.setup(
    name="ph",
    version="0.0.1",
    author="Pål Grønås Drange",
    license="MIT",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/pgdr/ph",
    entry_points={"console_scripts": ["ph=ph:main"]},
    packages=["ph"],
    install_requires=REQUIREMENTS,
)
