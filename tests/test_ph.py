import ph
import sys

import os.path


def _src(x):
    root = os.path.dirname(__file__)
    return os.path.abspath(os.path.join(root, x))


A_CSV = _src("test_data/a.csv")
A_CSV_CONTENT = ""
with open(A_CSV, "r") as fin:
    A_CSV_CONTENT = "".join(fin.readlines())


def test_cat(capsys):
    ph.cat(A_CSV)
    captured = capsys.readouterr()
    assert captured.out.strip() == A_CSV_CONTENT.strip()
    assert captured.err == ""
