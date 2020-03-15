import ph

import os.path
import sys
import io

import pytest
import contextlib

import pandas as pd


def _data(x):
    root = os.path.dirname(__file__)
    path = os.path.abspath(os.path.join(root, x))
    with open(path, "r") as fin:
        data = "".join(fin.readlines())
    return data


files = ("a", "iris", "covid")
DATA = {}
for f in files:
    DATA[f] = _data("test_data/{}.csv".format(f))


class Capture:
    # Just a mutable string container for ctx mgr around capture.out
    def __init__(self):
        self.out = ""

    @property
    def df(self):
        return pd.read_csv(io.StringIO(self.out))


@pytest.fixture
def phmgr(capsys, monkeypatch):
    @contextlib.contextmanager
    def phmgr(dataset="a"):
        monkeypatch.setattr("sys.stdin", io.StringIO(DATA[dataset]))
        cap = Capture()
        yield cap
        outerr = capsys.readouterr()
        cap.out, cap.err = outerr.out, outerr.err
        assert not cap.err, "Std error not empty: {}".format(cap.err)

    return phmgr


def test_cat(phmgr):
    with phmgr() as captured:
        ph.COMMANDS["cat"]()
    assert captured.out == DATA["a"]


def test_describe(phmgr):
    with phmgr() as captured:
        ph.COMMANDS["describe"]()
    assert len(captured.out.split("\n")) == 10
    header = set(captured.out.split("\n")[0].split())
    assert "x" in header
    assert "y" in header
    assert "max" in captured.out


def test_shape(phmgr):
    with phmgr("covid") as captured:
        ph.COMMANDS["shape"]()
    df = captured.df
    assert list(df.columns) == ["rows", "columns"]
    assert list(df["rows"]) == [29]
    assert list(df["columns"]) == [10]


def test_transpose(phmgr):
    with phmgr() as captured:
        ph.COMMANDS["transpose"]()
    assert (
        captured.out
        == """\
0,1,2,3,4,5
3,4,5,6,7,8
8,9,10,11,12,13
"""
    )


def test_median(phmgr):
    with phmgr() as captured:
        ph.COMMANDS["median"]()
    df = captured.df["0"]
    assert list(df) == [5.5, 10.5]


def test_head_tail(capsys, monkeypatch):
    monkeypatch.setattr("sys.stdin", io.StringIO(DATA["a"]))
    ph.COMMANDS["head"](7)
    captured = capsys.readouterr()
    assert not captured.err

    monkeypatch.setattr("sys.stdin", io.StringIO(captured.out))
    ph.COMMANDS["tail"](3)
    captured = capsys.readouterr()
    assert (
        captured.out
        == """\
x,y
6,11
7,12
8,13
"""
    )
    assert not captured.err


def test_date(phmgr):
    with phmgr() as captured:
        ph.COMMANDS["date"]("x")
    assert (
        captured.out
        == """\
x,y
1970-01-01 00:00:00.000000003,8
1970-01-01 00:00:00.000000004,9
1970-01-01 00:00:00.000000005,10
1970-01-01 00:00:00.000000006,11
1970-01-01 00:00:00.000000007,12
1970-01-01 00:00:00.000000008,13
"""
    )


def test_eval(phmgr):
    with phmgr() as captured:
        ph.COMMANDS["eval"]("x = x**2")
    assert (
        captured.out
        == """\
x,y
9,8
16,9
25,10
36,11
49,12
64,13
"""
    )


def test_version(phmgr):
    import ph._version

    with phmgr() as captured:
        ph.print_version()
    assert not captured.err
    assert captured.out == ph._version.__version__ + "\n"
