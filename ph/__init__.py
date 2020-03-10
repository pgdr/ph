#!/usr/bin/env python
from __future__ import print_function
from .tabulate import tabulate as tabulate_
import pandas as pd
import sys

USAGE_TEXT = """
ph is a command line tool for streaming csv data.

If you have a csv file `a.csv`, you can pipe it through `ph` on the
command line by using

$ cat a.csv | ph columns x y | ph tabulate

Use ph help [command] for help on the individual commands.

A list of available commands follows.
"""

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
def eval(expr):
    """Eval expr using pandas.DataFrame.eval.

    Example:  cat a.csv | ph eval "z = x + y"

    """
    df = pipein()
    pipeout(df.eval(expr))


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
    """Opens a file if provided (like `open`), or else cats it."""
    if fname is None:
        pipeout(pipein())
    else:
        pipeout(pd.read_csv(fname))


@register
def tab():
    pipeout(pipein(), sep="\t")


@register
def tabulate(*args, **kwargs):
    """Tabulate the output, meaning that the `ph` pipeline necessarily ends.

    Takes arguments --headers and --format=[grid, ...].

    This function uses the tabulate project.

    """
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
    """Writes help (docstring) about the different commands."""
    if not args:
        print("Usage: ph command arguments")
        print(USAGE_TEXT)
        print("       commands = {}".format(list(COMMANDS.keys())))
        exit(0)
    cmd = args[0]
    import ph

    try:
        fn = getattr(ph, cmd)
        ds = getattr(fn, "__doc__")
    except AttributeError as err:
        try:
            fn = getattr(pd.DataFrame, cmd)
            ds = getattr(fn, "__doc__")
        except AttributeError as err:
            ds = ""

    print("Usage: ph {} [?]".format(cmd))
    print("       {}".format(ds))


@register
def open(fname):
    """Open a csv file, similar to `cat`, except it needs one argument."""
    pipeout(pd.read_csv(fname))


def _call(attr, *args, **kwargs):
    pipeout(getattr(pipein(), attr)(*args, **kwargs))


def register_forward(attr):
    COMMANDS[attr] = lambda: _call(attr)


@register
def head(n=10):
    """Similar to `head` but keeps the header.

    Print the header followed by the first 10 (or n) lines of the stream to
    standard output.

    """
    _call("head", int(n))


@register
def tail(n=10):
    """Similar to `tail` but keeps the header.

    Print the header followed by the last 10 (or n) lines of the stream to
    standard output.

    """
    _call("tail", int(n))


@register
def rename(before, after):
    """Rename a column name.

    Usage:  ph rename before after

    Example:  cat a.csv | ph rename x a | ph rename y b

    """
    pipeout(pipein().rename(columns={before: after}))


@register
def columns(*cols):
    """ph columns servers two purposes.

    Called without any arguments, it lists the names of the columns in
    the stream.

    Called with arguments, it streams out the csv data from the given columns with prescribed order.

    `cat a.csv | ph columns c b` will print columns c and b to standard out,
    regardless of their order in a.csv.

    """
    if not cols:
        print("\n".join(list(pipein().columns)))
    else:
        pipeout(pipein()[list(cols)])


@register
def shape():
    """Print the shape of the csv file, i.e. num cols and num rows.

    The output will have two rows and two columns, with header "rows,columns".

    """
    print("rows,columns\n" + ",".join([str(x) for x in pipein().shape]))


@register
def empty():
    """Print a csv file with one column containing True or False.

    The output depends on whether the csv input was empty.

    """
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
