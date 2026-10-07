"""Regression tests for user-visible bugs in ph.

These tests intentionally describe the desired behaviour.  Against the current
1.1.6 sources, the bug-regression tests should fail until the corresponding
fixes are made.
"""

from __future__ import annotations

import io
import sys

import pytest

import ph


def run_with_stdin(monkeypatch, capsys, data, fn, *args, **kwargs):
    """Run a ph command function with CSV on stdin and return stdout."""
    monkeypatch.setattr(sys, "stdin", io.StringIO(data))
    fn(*args, **kwargs)
    return capsys.readouterr().out


def test_plot_with_index_preserves_index_column(monkeypatch, capsys, tmp_path):
    """plot must remain a CSV-preserving pipeline stage."""
    pytest.importorskip("matplotlib")

    data = "date,x\n2020-01-01,1\n2020-01-02,2\n"
    image = tmp_path / "plot.png"

    out = run_with_stdin(
        monkeypatch,
        capsys,
        data,
        ph.plot,
        index="date",
        savefig=str(image),
    )

    assert image.exists()
    assert out == data


@pytest.mark.parametrize("command", ["values", "axes", "size", "keys", "iterrows"])
def test_accidental_non_tabular_dataframe_api_is_not_a_command(command):
    """Do not expose pandas attributes/methods that cannot form a ph pipeline."""
    assert command not in ph.COMMANDS


@pytest.mark.parametrize("fmt", ["msgpack", "sas", "spss", "hdf5"])
def test_nonfunctional_writer_formats_are_not_advertised(fmt):
    """A writer that ph/pandas cannot execute should not be in the writer registry."""
    assert fmt not in ph.WRITERS


def test_documented_forwarded_commands_remain_available():
    """Pruning accidental pandas API exposure should not remove useful commands."""
    expected = {
        "abs",
        "corr",
        "count",
        "cov",
        "cummax",
        "cumsum",
        "max",
        "median",
        "quantile",
        "rank",
        "std",
        "sum",
        "transpose",
        "var",
    }
    assert expected <= set(ph.COMMANDS)
