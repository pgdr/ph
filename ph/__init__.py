#!/usr/bin/env python
from __future__ import print_function
from .tabulate import tabulate as tabulate_
import sys
import os.path
import pandas as pd
import re

# Command line parsing of (1) --abc and (2) --abc=def
KWARG = re.compile("^--[a-z0-9_-]+$")
KWARG_WITH_VALUE = re.compile("^--[a-z0-9_-]+=")


USAGE_TEXT = """
ph is a command line tool for streaming csv data.

If you have a csv file `a.csv`, you can pipe it through `ph` on the
command line by using

$ cat a.csv | ph columns x y | ph tabulate

Use ph help [command] for help on the individual commands.

A list of available commands follows.
"""

COMMANDS = {}

# These are all lambdas because they lazy load, and some of these
# readers are introduced in later pandas.
READERS = {
    "csv": lambda fname: pd.read_csv(fname),
    "fwf": lambda fname: pd.read_fwf(fname),
    "json": lambda fname: pd.read_json(fname),
    "html": lambda fname: pd.read_html(fname),
    "clipboard": lambda fname: pd.read_clipboard(fname),
    "xls": lambda fname: pd.read_excel(fname),
    "odf": lambda fname: pd.read_excel(fname),
    "hdf5": lambda fname: pd.read_hdf(fname),
    "feather": lambda fname: pd.read_feather(fname),
    "parquet": lambda fname: pd.read_parquet(fname),
    "orc": lambda fname: pd.read_orc(fname),
    "msgpack": lambda fname: pd.read_msgpack(fname),
    "stata": lambda fname: pd.read_stata(fname),
    "sas": lambda fname: pd.read_sas(fname),
    "spss": lambda fname: pd.read_spss(fname),
    "pickle": lambda fname: pd.read_pickle(fname),
    "sql": lambda fname: pd.read_sql(fname),
    "gbq": lambda fname: pd.read_gbq(fname),
    "google": lambda fname: pd.read_gbq(fname),
    "bigquery": lambda fname: pd.read_gbq(fname),
    ### extras
    "tsv": lambda fname: pd.read_csv(fname, sep="\t"),
}


def register(fn):
    COMMANDS[fn.__name__] = fn
    return fn


@register
def dataset(dset=None):
    """Load dataset as csv.

    Usage:  ph dataset linnerud | ph describe
    """
    try:
        import sklearn.datasets
    except ImportError:
        exit("You need scikit-learn.  Install ph[data].")

    REALDATA = {
        "olivetti_faces": sklearn.datasets.fetch_olivetti_faces,
        "20newsgroups": sklearn.datasets.fetch_20newsgroups,
        "20newsgroups_vectorized": sklearn.datasets.fetch_20newsgroups_vectorized,
        "lfw_people": sklearn.datasets.fetch_lfw_people,
        "lfw_pairs": sklearn.datasets.fetch_lfw_pairs,
        "covtype": sklearn.datasets.fetch_covtype,
        "rcv1": sklearn.datasets.fetch_rcv1,
        "kddcup99": sklearn.datasets.fetch_kddcup99,
        "california_housing": sklearn.datasets.fetch_california_housing,
    }

    TOYDATA = {
        "boston": sklearn.datasets.load_boston,
        "iris": sklearn.datasets.load_iris,
        "diabetes": sklearn.datasets.load_diabetes,
        "digits": sklearn.datasets.load_digits,
        "linnerud": sklearn.datasets.load_linnerud,
        "wine": sklearn.datasets.load_wine,
        "breast_cancer": sklearn.datasets.load_breast_cancer,
    }

    if dset is None:
        print("type,name")
        print("\n".join("{},{}".format("real", k) for k in REALDATA))
        print("\n".join("{},{}".format("toy", k) for k in TOYDATA))
        exit()

    if dset not in TOYDATA.keys() | REALDATA.keys():
        exit("Unknown dataset {}.  See ph help dataset.".format(dset))
    if dset in TOYDATA:
        data = TOYDATA[dset]()
    else:
        data = REALDATA[dset]()
    try:
        df = pd.DataFrame(data.data, columns=data.feature_names)
    except AttributeError:
        df = pd.DataFrame(data.data)
    try:
        df["target"] = pd.Series(data.target)
    except Exception:
        pass
    pipeout(df)


@register
def dropna():
    """Remove rows with N/A values"""
    pipeout(pipein().dropna())


def pipeout(df, sep=",", index=False, *args, **kwargs):
    try:
        print(df.to_csv(sep=sep, index=index, *args, **kwargs))
    except BrokenPipeError:
        try:
            sys.stdout.close()
        except IOError:
            pass
        try:
            sys.stderr.close()
        except IOError:
            pass


def pipein():
    try:
        return pd.read_csv(sys.stdin)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


@register
def query(expr):
    """Using pandas queries.

    Usage: cat a.csv | ph query "x > 5"

    """
    df = pipein()
    new_df = df.query(expr)
    pipeout(new_df)


@register
def appendstr(col, s, newcol=None):
    """Special method to append a string to the end of a column.

    Usage: cat e.csv | ph appendstr year -01-01 | ph date year
    """
    df = pipein()
    if newcol is None:
        newcol = col
    df[newcol] = df[col].astype(str) + s
    pipeout(df)


@register
def astype(type, column=None, newcolumn=None):
    """Cast a column to a different type.

    Usage:  cat a.csv | ph astype double x [new_x]

    """
    df = pipein()
    if column is None:
        df = df.astype(type)
    elif newcolumn is not None:
        df[newcolumn] = df[column].astype(type)
    else:
        df[column] = df[column].astype(type)
    pipeout(df)


@register
def dtypes(t=None):
    """If no argument is provided, output types, otherwise filter on types.

    If no argument is provided, output a csv with two columns, "column" and
    "dtype".  The "column" column contains the names of the columns in the input
    csv and the "dtype" column contains their respective types.

    If an argument is provided, all columns with the prescribed type is output.

    Usage:  cat a.csv | ph dtypes
            cat a.csv | ph dtypes float64

    """
    if t is None:
        df = pipein()
        newdf = pd.DataFrame(pd.Series(df.columns), columns=["column"])
        newdf["dtype"] = pd.Series([str(e) for e in df.dtypes])
        pipeout(newdf)
    else:
        df = pipein().select_dtypes(t)
        pipeout(df)


@register
def monotonic(column, direction="+"):
    """Check if a certain column is monotonically increasing or decreasing.

    Usage:  cat a.csv | ph monotonic x
            cat a.csv | ph monotonic x +  # equivalent to above
            cat a.csv | ph monotonic x -  # for decreasing

    """
    df = pipein()
    if column not in df:
        exit("Unknown column {}".format(column))
    if direction not in "+-":
        exit("direction must be either + or -")
    print("{}_monotonic".format(column))
    if direction == "+":
        print(df[column].is_monotonic)
    else:
        print(df[column].is_monotonic_decreasing)


@register
def plot(*args, **kwargs):
    """Plot the csv file.

    Usage:  ph plot
            ph plot --index=col
            ph plot --kind=bar
            ph plot --kind=scatter --x=col1 --y=col2
            ph plot --style=k--
    """
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        exit("plot depends on matplotlib, install ph[plot]")

    df = pipein()
    index = kwargs.get("index")
    if index is not None:
        df = df.set_index(index)
    df.plot(
        kind=kwargs.get("kind", "line"),  # default pandas plot is line
        style=kwargs.get("style"),
        x=kwargs.get("x"),
        y=kwargs.get("y"),
    )
    plt.show()
    pipeout(df)


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
    """Tabulate the output for pretty-printing.

    Usage: cat a.csv | ph tabulate --headers --noindex --format=grid

    Takes arguments
      * --headers
      * --noindex
      * --format=[grid, latex, pretty, ...].

    For a full list of format styles confer the README.

    This function uses the tabulate project available as a standalone
    package from PyPI.

    Using `tabulate` in a pipeline usually means that the `ph` pipeline ends.
    This is because of `tabulate`'s focus on user readability over machine
    readability.

    """
    headers = tuple()
    fmt = kwargs.get("format")
    index = True
    if "--noindex" in args:
        index = False
    if "--headers" in args:
        headers = "keys"
    print(tabulate_(pipein(), tablefmt=fmt, headers=headers, showindex=index))


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
def open(ftype, fname):
    """Open an ftype file fname and stream out.

    Usage:  `ph open csv a.csv`
            `ph open odf a.ods`
            `ph open xls a.xls[x]`
            `ph open parquet a.parquet`


    """
    if ftype not in READERS:
        exit("Unknown filetype {}".format(ftype))
    reader = READERS[ftype]
    try:
        df = reader(fname)
    except AttributeError as err:
        exit(
            "{} is not supported in your Pandas installation\n{}".format(
                ftype, err
            )
        )
    except ImportError as err:
        exit(
            "{} is not supported in your Pandas installation\n{}".format(
                ftype, err
            )
        )
    pipeout(df)


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


def __tryparse(x):
    x_ = x
    try:
        x_ = float(x)
        x_ = int(x)
    except ValueError:
        pass
    return x_


@register
def replace(column, old, new, newcolumn=None):
    """Replace a value in a column with a new value.

    Usage: cat a.csv | ph replace y 8 100
           cat a.csv | ph replace y 8 100 z

    Beware that it is difficult to know which _types_ we are searching for,
    therefore we only apply a heuristic, which is doomed to be faulty.
    """
    if newcolumn is None:
        newcolumn = column
    df = pipein()
    if df[column].dtype != object:
        old = __tryparse(old)
        new = __tryparse(new)
    if column not in df:
        exit("Column {} does not exist.".format(column))
    df[newcolumn] = df[column].replace(to_replace=old, value=new, inplace=False)
    pipeout(df)


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
    if len(sys.argv) < 2:
        exit("Usage: ph command [args]\n       ph help")
    cmd = sys.argv[1]
    if cmd not in COMMANDS:
        exit("Unknown command {}.".format(cmd))

    # Self-implemented parsing of arguments.
    # Arguments of type "abc" and "--abc" go into args
    # Arguments of type "--abc=def" go into kwargs as key, value pairs
    args = []
    kwarg = {}
    for a in sys.argv[2:]:
        if KWARG.match(a):
            args.append(a)
        elif KWARG_WITH_VALUE.match(a):
            split = a.index("=")
            k = a[2:split]
            v = a[split + 1 :]
            kwarg[k] = v
        else:
            args.append(a)
    COMMANDS[cmd](*args, **kwarg)


if __name__ == "__main__":
    main()
