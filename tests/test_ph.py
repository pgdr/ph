import ph
import sys

import os.path
import io


def _src(x):
    root = os.path.dirname(__file__)
    return os.path.abspath(os.path.join(root, x))


A_CSV = _src("test_data/a.csv")
A_CSV_CONTENT = ""
with open(A_CSV, "r") as fin:
    A_CSV_CONTENT = "".join(fin.readlines())


def test_cat(capsys, monkeypatch):
    monkeypatch.setattr("sys.stdin", io.StringIO(A_CSV_CONTENT))
    ph.COMMANDS["cat"]()
    captured = capsys.readouterr()
    assert captured.out == A_CSV_CONTENT
    assert not captured.err


def test_describe(capsys, monkeypatch):
    monkeypatch.setattr("sys.stdin", io.StringIO(A_CSV_CONTENT))
    ph.COMMANDS["describe"]()
    captured = capsys.readouterr()
    assert len(captured.out.split("\n")) == 10
    header = set(captured.out.split("\n")[0].split())
    assert "x" in header
    assert "y" in header
    assert "max" in captured.out
    assert not captured.err


def test_transpose(capsys, monkeypatch):
    monkeypatch.setattr("sys.stdin", io.StringIO(A_CSV_CONTENT))
    ph.COMMANDS["transpose"]()
    captured = capsys.readouterr()
    assert (
        captured.out
        == """\
0,1,2,3,4,5
3,4,5,6,7,8
8,9,10,11,12,13
"""
    )
    assert not captured.err


def test_median(capsys, monkeypatch):
    monkeypatch.setattr("sys.stdin", io.StringIO(A_CSV_CONTENT))
    ph.COMMANDS["median"]()
    captured = capsys.readouterr()
    assert (
        captured.out
        == """\
0
5.5
10.5
"""
    )
    assert not captured.err


def test_head_tail(capsys, monkeypatch):
    monkeypatch.setattr("sys.stdin", io.StringIO(A_CSV_CONTENT))
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


def test_date(capsys, monkeypatch):
    monkeypatch.setattr("sys.stdin", io.StringIO(A_CSV_CONTENT))
    ph.COMMANDS["date"]("x")
    captured = capsys.readouterr()
    assert not captured.err
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


def test_eval(capsys, monkeypatch):
    monkeypatch.setattr("sys.stdin", io.StringIO(A_CSV_CONTENT))
    ph.COMMANDS["eval"]("x = x**2")
    captured = capsys.readouterr()
    assert not captured.err
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
