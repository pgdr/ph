#!/usr/bin/env python
from __future__ import print_function
from .tabulate import tabulate as tabulate_
import pandas as pd
import sys

COMMANDS = {}


def register(fn):
    COMMANDS[fn.__name__] = fn
    return fn


def pipeout(df, sep=",", index=False, *args, **kwargs):
    print(df.to_csv(sep=sep, index=index, *args, **kwargs))


def pipein():
    try:
        return pd.read_csv(sys.stdin)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


@register
def apply(op, col1, col2, col3):
    df = pipein()
    if op in ("+", "add"):
        df[col3] = df[col1] + df[col2]
    elif op in ("*", "mul"):
        df[col3] = df[col1] * df[col2]
    elif op in ("-", "sub"):
        df[col3] = df[col1] - df[col2]
    elif op in ("**", "pow"):
        df[col3] = df[col1] ** df[col2]
    else:
        exit("Unknown operation {}".format(op))
    pipeout(df)


@register
def normalize(col=None):
    df = pipein()
    if col is None:
        df = (df - df.min()) / (df.max() - df.min())
    else:
        df[col] = (df[col] - df[col].min()) / (df[col].max() - df[col].min())
    pipeout(df)


@register
def date(col):
    df = pipein()
    df[col] = pd.to_datetime(df[col])
    pipeout(df)


@register
def describe():
    print(pipein().describe())


@register
def cat(fname=None):
    if fname is None:
        pipeout(pipein())
    else:
        pipeout(pd.read_csv(fname))


@register
def tab():
    pipeout(pipein(), sep="\t")


@register
def tabulate(*args, **kwargs):
    headers = tuple()
    fmt = None
    if "--headers" in args:
        headers = "keys"
    for opt in args:
        if opt.startswith("--format="):
            fmt = opt.split("--format=")[1]
            break
    print(tabulate_(pipein(), tablefmt=fmt, headers=headers))


@register
def help(*args, **kwargs):
    print("Usage: ph command arguments")
    print("       commands = {}".format(list(COMMANDS.keys())))


@register
def open(fname):
    pipeout(pd.read_csv(fname))


def _call(attr, *args, **kwargs):
    pipeout(getattr(pipein(), attr)(*args, **kwargs))


def register_forward(attr):
    COMMANDS[attr] = lambda: _call(attr)


@register
def head(n=10):
    _call("head", int(n))


@register
def tail(n=10):
    _call("tail", int(n))


@register
def columns(*cols):
    pipeout(pipein()[list(cols)])


@register
def shape():
    print("rows,columns\n" + ",".join([str(x) for x in pipein().shape]))


@register
def empty():
    print("empty\n{}".format(pipein().empty))


pandas_computations = [
    "abs",
    "all",
    "any",
    "clip",
    "corr",
    "count",
    "cov",
    "cummax",
    "cummin",
    "cumprod",
    "cumsum",
    "diff",
    "kurt",
    "kurtosis",
    "mad",
    "max",
    "mean",
    "median",
    "min",
    "mode",
    "pct_change",
    "prod",
    "product",
    "quantile",
    "rank",
    "round",
    "sem",
    "skew",
    "sum",
    "std",
    "var",
    "nunique",
    "transpose",
]
for attr in pandas_computations:
    register_forward(attr)


def main():
    args = sys.argv
    if len(args) < 2:
        exit("Usage: ph command [args]\n       ph help")
    cmd = args[1]
    if cmd not in COMMANDS:
        exit("Unknown command {}.".format(cmd))
    COMMANDS[cmd](*args[2:])


if __name__ == "__main__":
    main()
