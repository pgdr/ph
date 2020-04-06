#!/usr/bin/env python
from __future__ import print_function
from .tabulate import tabulate as tabulate_
import sys
import pandas as pd
import re


def _get_version():
    import ph._version

    return ph._version.__version__


def print_version():
    print(_get_version())


# Command line parsing of (1) --abc and (2) --abc=def
KWARG = re.compile("^--[a-z0-9_-]+$")
KWARG_WITH_VALUE = re.compile("^--[a-z0-9_-]+=")


USAGE_TEXT = """
ph is a command line tool for streaming csv data.

If you have a csv file `a.csv`, you can pipe it through `ph` on the
command line by using

$ cat a.csv | ph columns x y | ph eval "z = x**2 - y" | ph show

Use ph help [command] for help on the individual commands.

A list of available commands follows.
"""

COMMANDS = {}


def _gpx(fname):
    try:
        import gpxpy
    except ImportError:
        exit("ph gpx needs pgxpy, pip install ph[gpx]")

    def from_trackpoint(tp=None):
        if tp is None:
            return "time", "latitude", "longitude", "elevation", "distance"
        p = tp.point
        return str(p.time), p.latitude, p.longitude, p.elevation, tp.distance_from_start

    with open(fname, "r") as fin:
        gpx = gpxpy.parse(fin)
    data = gpx.get_points_data()
    columns = from_trackpoint()
    dfdata = [from_trackpoint(tp) for tp in data]
    return pd.DataFrame(dfdata, columns=columns)


def _tsv(*args, **kwargs):
    kwargs["sep"] = "\t"
    return pd.read_csv(*args, **kwargs)


# These are all lambdas because they lazy load, and some of these
# readers are introduced in later pandas.
READERS = {
    "csv": pd.read_csv,
    "clipboard": pd.read_clipboard,
    "fwf": pd.read_fwf,
    "json": pd.read_json,
    "html": pd.read_html,
    "excel": lambda fname, sheet: pd.read_excel(fname, sheet),
    "xls": lambda fname, sheet: pd.read_excel(fname, sheet),
    "odf": lambda fname, sheet: pd.read_excel(fname, sheet),
    "tsv": _tsv,
    "gpx": _gpx,
}


try:
    READERS["hdf5"] = pd.read_hdf
except AttributeError:
    pass


try:
    READERS["feather"] = pd.read_feather
except AttributeError:
    pass


try:
    READERS["parquet"] = pd.read_parquet
except AttributeError:
    pass


try:
    READERS["orc"] = pd.read_orc
except AttributeError:
    pass


try:
    READERS["msgpack"] = pd.read_msgpack
except AttributeError:
    pass


try:
    READERS["stata"] = pd.read_stata
except AttributeError:
    pass


try:
    READERS["sas"] = pd.read_sas
except AttributeError:
    pass


try:
    READERS["spss"] = pd.read_spss
except AttributeError:
    pass


try:
    READERS["pickle"] = pd.read_pickle
except AttributeError:
    pass


try:
    READERS["gbq"] = pd.read_gbq
except AttributeError:
    pass


try:
    READERS["google"] = pd.read_gbq
except AttributeError:
    pass


try:
    READERS["bigquery"] = pd.read_gb
except AttributeError:
    pass


WRITERS = {
    "csv": "to_csv",
    "fwf": "to_fwf",
    "json": "to_json",
    "html": "to_html",
    "clipboard": "to_clipboard",
    "xls": "to_excel",
    "odf": "to_excel",
    "hdf5": "to_hdf",
    "feather": "to_feather",
    "parquet": "to_parquet",
    "orc": "to_orc",
    "msgpack": "to_msgpack",
    "stata": "to_stata",
    "sas": "to_sas",
    "spss": "to_spss",
    "pickle": "to_pickle",
    "gbq": "to_gbq",
    "google": "to_gbq",
    "bigquery": "to_gbq",
    # extras
    "tsv": "to_csv",
}


FALSY = ("False", "false", "No", "no", "0", False, 0)
TRUTHY = ("True", "true", "Yes", "yes", "1", True, 1)


def register(fn, name=None):
    if name is None:
        name = fn.__name__
    COMMANDS[name] = fn
    return fn


def registerx(name):
    def inner(fn):
        register(fn, name)
        return fn

    return inner


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
def dropna(axis=0, how="any", thresh=None):
    """Remove rows (or columns) with N/A values.

    Argument: --axis=0
    Defaults to axis=0 (columns), use --axis=1 to remove rows.

    Argument: --how=any
    Defaults to how='any', which removes columns (resp. rows) containing
    nan values.  Use how='all' to remove columns (resp. rows) containing
    only nan values.

    Argument: --thresh=5
    If --thresh=x is specified, will delete any column (resp. row) with
    fewer than x non-na values.

    Usage: cat a.csv | ph dropna
           cat a.csv | ph dropna --axis=1    # for row-wise
           cat a.csv | ph dropna --thresh=5  # keep cols with >= 5 non-na
           cat a.csv | ph dropna --how=all   # delete only if all vals na

    """
    try:
        axis = int(axis)
        if axis not in (0, 1):
            exit("ph dropna --axis=0 or --axis=1, not {}".format(axis))
    except ValueError:
        exit("ph dropna --axis=0 or --axis=1, not {}".format(axis))

    if thresh is not None:
        try:
            thresh = int(thresh)
        except ValueError:
            exit("ph dropna --thresh=0 or --thresh=1, not {}".format(thresh))

    df = pipein()
    try:
        df = df.dropna(axis=axis, how=how, thresh=thresh)
    except Exception as err:
        exit(err)
    pipeout(df)


def pipeout(df, sep=",", index=False, *args, **kwargs):
    csv = df.to_csv(sep=sep, index=index, *args, **kwargs)
    output = csv.rstrip("\n")
    try:
        print(output)
    except BrokenPipeError:
        try:
            sys.stdout.close()
        except IOError:
            pass
        try:
            sys.stderr.close()
        except IOError:
            pass


def pipein(ftype="csv", **kwargs):
    skiprows = kwargs.get("skiprows")
    if skiprows is not None:
        try:
            skiprows = int(skiprows)
            if skiprows < 0:
                raise ValueError("Negative")
        except ValueError:
            exit("skiprows must be a non-negative int, not {}".format(skiprows))
        kwargs["skiprows"] = skiprows

    if kwargs.get("sep") == "\\t":
        kwargs["sep"] = "\t"

    try:
        return READERS[ftype](sys.stdin, **kwargs)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


@register
def fillna(value=None, method=None, limit=None):
    """Fill na values with a certain value or method, at most `limit` many.

    Takes either a value, or a method using (e.g.) --method=ffill.

    Argument: value
    If provided, replaces all N/A values with prescribed value.

    Argument: --method=pad
    If provided, value cannot be provided.  Allowed methods are
    backfill, bfill, pad, ffill

    Argument: --limit=x
    If provided, limits number of consecutive N/A values to fill.


    Usage: cat a.csv | ph fillna 999.75
           cat a.csv | ph fillna -1
           cat a.csv | ph fillna --method=pad
           cat a.csv | ph fillna --method=pad --limit=5

    """
    if limit is not None:
        try:
            limit = int(limit)
        except ValueError:
            exit("--limit=x must be an integer, not {}".format(limit))
    METHODS = ("backfill", "bfill", "pad", "ffill")
    if method is not None:
        if method not in METHODS:
            exit("method must be one of {}, not {}".format(METHODS, method))
        pipeout(pipein().fillna(method=method, limit=limit))
    elif value is not None:
        value = __tryparse(value)
        pipeout(pipein().fillna(value=value, limit=limit))
    else:
        exit("'ph dropna' needs exactly one of value and method")


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
    try:
        if column is None:
            df = df.astype(type)
        elif newcolumn is not None:
            df[newcolumn] = df[column].astype(type)
        else:
            df[column] = df[column].astype(type)
    except ValueError as err:
        exit("Could not convert to {}: {}".format(type, err))
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
        pipeout(newdf.T, header=False)
    else:
        df = pipein().select_dtypes(t)
        pipeout(df)


@register
def pivot(columns, index=None, values=None):
    """Reshape csv organized by given index / column values.

    Suppose b.csv is
foo,bar,baz,zoo
one,A,1,x
one,B,2,y
one,C,3,z
two,A,4,q
two,B,5,w
two,C,6,t

    Usage: cat b.csv | ph pivot bar --index=foo --values=baz

      A    B    C
--  ---  ---  ---
 0    1    2    3
 1    4    5    6

    """
    pipeout(pipein().pivot(index=index, columns=columns, values=values))


@register
def crosstab(column):
    """Perform a very simplistic crosstabulation on one column of the input csv.

    Usage:  cat b.csv | ph crosstab foo
    """
    newcol = "crosstab_{}".format(column)
    df = pd.crosstab(pipein()[column], newcol)
    df["id"] = list(df[newcol].index)
    pipeout(df)


@register
def groupby(*columns, how="sum", as_index=True):
    """Group by columns, then apply `how` function.

    Usage: cat a.csv | ph groupby animal --how=mean
           cat a.csv | ph groupby animal --how=mean --as_index=False
    """
    columns = list(columns)
    if not columns:
        exit("Needs at least one column to group by")
    df = pipein()
    for c in columns:
        if c not in df.columns:
            exit("Unknown column name {}".format(c))
    if as_index in TRUTHY:
        as_index = True
    elif as_index in FALSY:
        as_index = False
    else:
        exit("--as_index=True or False, not {}".format(as_index))

    grouped = df.groupby(columns, as_index=as_index)
    try:
        fn = getattr(grouped, how)
    except AttributeError:
        exit("Unknown --how={}, should be sum, mean, ...".format(how))
    retval = fn()

    pipeout(retval)


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
def iplot(*args, **kwargs):
    """Use plotly/cufflinks for interactive plot.

    This option is similar to `plot` but creates an HTML file and opens a
    browser for an interactive plot.

    Usage: cat a.csv | ph iplot
           cat a.csv | ph iplot --kind=bar
           cat a.csv | ph iplot --kind=bar --barmode=stack


    Depends on cufflinks: pip install ph[iplot].

    """
    try:
        import cufflinks  # noqa
        import plotly as py
    except ImportError:
        exit("iplot needs cufflinks, pip install ph[iplot]")

    df = pipein()
    fig = df.iplot(*args, asFigure=True, **kwargs)
    py.offline.plot(fig)
    pipeout(df)


@register
def plot(*args, **kwargs):
    """Plot the csv file.

    Usage:  ph plot
            ph plot --index=col
            ph plot --kind=bar
            ph plot --kind=scatter --x=col1 --y=col2
            ph plot --style=k--
            ph plot --logx=True
            ph plot --logy=True
            ph plot --loglog=True

    """
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        exit("plot depends on matplotlib, install ph[plot]")

    df = pipein()
    index = kwargs.get("index")
    if index is not None:
        df = df.set_index(index)
    for log_ in ("logx", "logy", "loglog"):
        if kwargs.get(log_) in TRUTHY:
            kwargs[log_] = True

    df.plot(
        kind=kwargs.get("kind", "line"),  # default pandas plot is line
        style=kwargs.get("style"),
        x=kwargs.get("x"),
        y=kwargs.get("y"),
        logx=kwargs.get("logx"),
        logy=kwargs.get("logy"),
        loglog=kwargs.get("loglog"),
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
    """Normalize a column or an entire dataframe.

    Usage: cat a.csv | ph normalize
           cat a.csv | ph normalize x


    Warning:  This is probably not what you want.

    """
    df = pipein()
    if col is None:
        df = (df - df.min()) / (df.max() - df.min())
    else:
        df[col] = (df[col] - df[col].min()) / (df[col].max() - df[col].min())
    pipeout(df)


@register
def date(col=None, unit=None, origin="unix", errors="raise", dayfirst=False):
    """Assemble datetime from multiple columns or from one column

    --unit can be D, s, us, ns (defaults to ns, ns from origin)

    --origin can be unix, julian, or time offset, e.g. '2000-01-01'

    --errors can be raise, coerce, ignore (see pandas.to_datetime)

    Usage: cat a.csv | ph date x
           cat a.csv | ph date x --unit=s --origin="1984-05-17 09:30"
           cat a.csv | ph date x --dayfirst=True
           cat a.csv | ph date  # if a.csv contains year, month, date

    """
    DATE_ERRORS = ("ignore", "raise", "coerce")
    if errors not in DATE_ERRORS:
        exit("Errors must be one of {}, not {}.".format(DATE_ERRORS, errors))

    dayfirst = dayfirst in TRUTHY

    df = pipein()
    try:
        if col is None:
            df = pd.to_datetime(df, unit=unit, origin=origin, errors=errors)
        else:
            if col not in df.columns:
                exit("No such column {}".format(col))
            df[col] = pd.to_datetime(
                df[col], unit=unit, origin=origin, errors=errors, dayfirst=dayfirst
            )
    except Exception as err:
        exit(err)

    pipeout(df)


@register
def describe():
    """Run DataFrame's describe method.

    The result is NOT tabular data, so pipeline ends.

    Usage: cat a.csv | ph describe
    """
    try:
        print(pipein().describe())
    except ValueError as err:
        exit(err)


@register
def info():
    """Run DataFrame's info method.

    The result is NOT tabular data, so pipeline ends.

    Usage: cat a.csv | ph info
    """
    print(pipein().info())


@register
def to(ftype, fname=None, sep=None, index=False):
    """Export csv to given format (possibly csv).

    Supports csv, html, json, parquet, bigquery, tsv, etc. (see README for full
    list).

    Usage: cat a.csv | ph to html
           cat a.csv | ph to tsv
           cat a.csv | ph to csv --index=True
           cat a.csv | ph to csv --sep=';'
           cat a.csv | ph to clipboard
           cat a.csv | ph to json
           cat a.csv | ph to parquet out.parquet

    """
    if ftype not in WRITERS:
        exit("Unknown datatype {}.".format(ftype))

    if not fname:
        if ftype in ("parquet", "xls", "xlsx", "ods", "pickle"):
            exit("{} needs a path".format(ftype))

    if ftype == "hdf5":
        exit("hdf5 writer not implemented")

    if index not in TRUTHY + FALSY:
        exit("Index must be True or False, not {}".format(index))
    index = index in TRUTHY

    if ftype == "fwf":
        # pandas has not yet implemented to_fwf
        df = pipein()
        content = tabulate_(df.values.tolist(), list(df.columns), tablefmt="plain")
        if fname:
            with open(fname, "w") as wout:
                wout.write(content)
        else:
            print(content)
        exit()

    if sep is not None:
        if ftype != "csv":
            exit("Only csv mode supports separator")

    writer = WRITERS[ftype]
    df = pipein()
    fn = getattr(df, writer)
    kwargs = {}
    if ftype == "tsv":
        kwargs["sep"] = "\t"
    elif ftype == "csv" and sep is not None:
        kwargs["sep"] = sep

    if ftype == "json":
        index = True

    if fname is not None:
        print(fn(fname, index=index, **kwargs))
    else:
        print(fn(index=index, **kwargs))


@registerx("from")
def from_(ftype="csv", **kwargs):
    """Read a certain (default csv) format from standard in and stream out as csv.

    Usage: cat a.json | ph from json

    The following pipes should be equivalent:

    cat a.csv
    cat a.csv | ph to json | ph from json
    cat a.tsv | ph from tsv
    cat a.tsv | ph from csv --sep='\t'
    cat a.tsv | ph from csv --sep='\t' --thousands=','


    """
    skiprows = kwargs.get("skiprows")
    if skiprows is not None:
        try:
            skiprows = int(skiprows)
            if skiprows < 0:
                raise ValueError("Negative")
        except ValueError:
            exit("skiprows must be a non-negative int, not {}".format(skiprows))
        kwargs["skiprows"] = skiprows

    if kwargs.get("sep") == "\\t":
        kwargs["sep"] = "\t"

    if ftype == "clipboard":
        pipeout(READERS["clipboard"](**kwargs))
        return

    pipeout(pipein(ftype, **kwargs))


@register
def cat(*fnames, axis="index"):
    """Concatenates all files provided.

    Usage: ph cat a.csv b.csv c.csv
           ph cat a.csv b.csv c.csv --axis=index  # default
           ph cat a.csv b.csv c.csv --axis=columns

    If no arguments are provided, read from std in.

    """
    if axis not in ("index", "columns"):
        exit("Unknown axis command '{}'".format(axis))
    if not fnames:
        pipeout(pipein())
    else:
        dfs = []
        for fname in fnames:
            df = pd.read_csv(fname)
            dfs.append(df)
        retval = pd.concat(dfs, axis=axis)
        pipeout(retval)


@register
def merge(fname1, fname2, how="inner", on=None):
    """
    Merging two csv files.

    Usage: ph merge a.csv b.csv --on=ijk

    """
    hows = ("left", "right", "outer", "inner")
    if how not in hows:
        exit("Unknown merge --how={}, must be one of {}".format(how, hows))
    df1 = pd.read_csv(fname1)
    df2 = pd.read_csv(fname2)
    if on is None:
        pipeout(pd.merge(df1, df2, how=how))
    else:
        pipeout(pd.merge(df1, df2, how=how, on=on))


@register
def tab():
    """Equivalent to `ph to tsv`.

    Usage: cat a.csv | ph tab
    """
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
def show(noindex=False):
    """Similar to ph tabulate --headers [--noindex].

    Usage: cat a.csv | ph show
           cat a.csv | ph show --noindex
    """
    if noindex:
        tabulate("--headers", "--noindex")
    else:
        tabulate("--headers")


def _print_commands(cmds):
    num_cols = 72 // max(len(cmd) for cmd in cmds)
    while (len(cmds) % num_cols) != 0:
        cmds.append("")
    df = pd.DataFrame(pd.Series(cmds).values.reshape(num_cols, -1))
    print(tabulate_(df.transpose(), showindex=False))


@register
def help(*args, **kwargs):
    """Writes help (docstring) about the different commands."""
    if not args:
        print("Usage: ph command arguments")
        print(USAGE_TEXT)
        _print_commands(sorted(COMMANDS.keys()))
        exit(0)
    cmd = args[0]
    import ph

    try:
        fn = getattr(ph, cmd)
        ds = getattr(fn, "__doc__")
    except AttributeError:
        try:
            fn = getattr(pd.DataFrame, cmd)
            ds = getattr(fn, "__doc__")
        except AttributeError:
            ds = ""

    print("Usage: ph {} [?]".format(cmd))
    print("       {}".format(ds))


def slugify_name(name):
    if not name:
        return "unnamed"
    if name == "_":
        return "_"
    lead_under = name[0] == "_"
    trail_under = name[-1] == "_"

    name = name.strip().lower()
    unwanted = set(c for c in name if not c.isalnum())
    for u in unwanted:
        name = name.replace(u, "_").strip()
    while "__" in name:
        name = name.replace("__", "_").strip()
    name = name.strip("_")
    if lead_under:
        name = "_" + name
    if trail_under:
        name = name + "_"
    return name


@register
def slugify():
    """Slugify the column headers.

    Usage: cat a.csv | ph slugify

    Removes all non-alphanumeric characters aside from the underscore.

    Is useful in scenarios where you have possibly many columns with
    very ugly names.  Can be a good preprocessor to @rename:

    Usage: cat a.csv | ph slugify | ph rename less_bad_name good_name

    """
    df = pipein()
    df.columns = [slugify_name(name) for name in df.columns]
    pipeout(df)


@registerx("open")
def open_(ftype, fname, **kwargs):
    """Use a reader to open a file.

    Open ftype file with name fname and stream out.

    Usage: ph open csv a.csv
           ph open csv a.csv --skiprows=7
           ph open json a.json
           ph open parquet a.parquet
           ph open excel a.ods
           ph open excel a.xls
           ph open excel a.xlsx
           ph open excel a.xls --sheet=2
           ph open excel a.xls --sheet="The Real Dataset sheet"
           ph open csv a.csv --thousands=','


    """
    if ftype not in READERS:
        exit("Unknown filetype {}".format(ftype))
    reader = READERS[ftype]

    sheet = kwargs.get("sheet")
    if ftype in ("excel", "xls", "odf"):
        kwargs["sheet"] = __tryparse(sheet)

    if kwargs.get("sep") == "\\t":
        kwargs["sep"] = "\t"

    if ftype == "clipboard" and fname is not None:
        exit("clipboard does not take fname")
    if ftype != "clipboard" and fname is None:
        exit("filename is required for {}".format(ftype))

    skiprows = kwargs.get("skiprows")
    if skiprows is not None:
        try:
            skiprows = int(skiprows)
            if skiprows < 0:
                raise ValueError("Negative")
        except ValueError:
            exit("skiprows must be a non-negative int, not {}".format(skiprows))
        kwargs["skiprows"] = skiprows

    try:
        if ftype == "clipboard":
            df = reader(**kwargs)
        else:
            df = reader(fname, **kwargs)
    except AttributeError as err:
        exit("{} is not supported in your Pandas installation\n{}".format(ftype, err))
    except ImportError as err:
        exit("{} is not supported in your Pandas installation\n{}".format(ftype, err))
    except FileNotFoundError as err:
        exit("File not found: {}".format(err))
    pipeout(df)


_ATTRS_WITH_SERIES_OUTPUT = (
    "all",
    "any",
    "count",
    "kurt",
    "kurtosis",
    "mad",
    "mean",
    "median",
    "min",
    "nunique",
    "prod",
    "product",
    "quantile",
    "sem",
    "skew",
    "std",
    "sum",
    "var",
)


def _call(attr, *args, **kwargs):
    df = pipein()
    dfn = getattr(df, attr)(*args, **kwargs)
    if attr in _ATTRS_WITH_SERIES_OUTPUT:
        dfn = dfn.reset_index()
        dfn = dfn.T
        pipeout(dfn, header=False)
    else:
        pipeout(dfn)


def register_forward(attr):
    COMMANDS[attr] = lambda: _call(attr)


@register
def head(n=10):
    """Similar to `head` but keeps the header.

    Print the header followed by the first 10 (or n) lines of the stream to
    standard output.

    Usage: cat a.csv | ph head
           cat a.csv | ph head 8


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
def columns(*cols, **kwargs):
    """ph columns servers two purposes.

    Called without any arguments, it lists the names of the columns in
    the stream.

    Called with arguments, it streams out the csv data from the given columns
    with prescribed order.

    Takes also arguments --startswith=the_prefix and --endswith=the_suffix which
    selects all columns matching either pattern.


    Usage: cat a.csv | ph columns      # will list all column names
           cat a.csv | ph columns y x  # select only columns y and x
           cat a.csv | ph columns --startswith=sepal

    """
    cols = list(cols)
    df = pipein()
    if "startswith" in kwargs:
        q = kwargs["startswith"]
        for col in df.columns:
            if col.startswith(q) and col not in cols:
                cols.append(col)
    if "endswith" in kwargs:
        q = kwargs["endswith"]
        for col in df.columns:
            if col.endswith(q) and col not in cols:
                cols.append(col)

    for col in cols:
        if col not in df.columns:
            exit("No such column {}".format(col))

    if not cols and not kwargs:
        print("columns")
        print("\n".join(list(df.columns)))
    else:
        pipeout(df[cols])


@register
def drop(*columns, **kwargs):
    """Drop specified labels from rows or columns.

    Remove rows or columns by specifying label names and corresponding
    axis, or by specifying directly index or column names.

    Usage: cat a.csv | ph drop 'x' --axis=columns
           cat a.csv | ph drop 0   --axis=index

    """
    for opt in ("axis", "levels"):
        if opt in kwargs:
            kwargs[opt] = __tryparse(kwargs[opt])
    if "inplace" in kwargs:
        exit("inplace in nonsensical in ph")

    df = pipein()

    if kwargs.get("axis") in (None, 0, "index"):
        columns = [__tryparse(col) for col in columns]
    elif kwargs.get("axis") in (1, "columns"):
        for col in columns:
            if col not in df.columns:
                exit("Unknown column {}.".format(col))
    else:
        exit(
            "--axis=index (or 0) or --axis=columns (or 1), not {}".format(
                kwargs.get("axis")
            )
        )

    ndf = df.drop(list(columns), **kwargs)
    pipeout(ndf)


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


@register
def index():
    """Reset the index to a 0..n-1 counter.

    Usage: cat a.csv | ph index

    Adds a left-most column `index`.
    """
    pipeout(pipein().reset_index())


@register
def sort(col):
    """Sort csv input by column.

    Usage: cat iris.csv | ph sort setosa

    """
    df = pipein()
    if col not in df.columns:
        exit("Unknown column {}".format(col))
    pipeout(df.sort_values(col))


@register
def polyfit(x, y, deg=1):
    """Perform linear/polynomial regression.

    Usage: cat a.csv | ph polyfix x y
           cat a.csv | ph polyfix x y --deg=1  # default
           cat a.csv | ph polyfix x y --deg=2  # default

    Outputs a column polyfit_{deg} containing the evaluated index.

    """
    df = pipein()
    if x not in df.columns:
        exit("Unknown column x={}".format(x))
    if y not in df.columns:
        exit("Unknown column y={}".format(y))
    deg = __tryparse(deg)
    if not isinstance(deg, int) or deg <= 0:
        exit("deg={} should be a positive int".format(deg))
    try:
        import numpy
    except ImportError:
        exit("numpy needed for polyfit.  pip install numpy")
    polynomial = numpy.polyfit(df[x], df[y], deg=deg)
    f = numpy.poly1d(polynomial)

    df["polyfit_{}".format(deg)] = df[x].apply(f)
    pipeout(df)


def __process(attr):
    if attr in COMMANDS:
        return False
    if attr.startswith("_"):
        return False
    if attr.startswith("to_"):
        return False
    return True


for attr in dir(pd.DataFrame):
    if __process(attr):
        register_forward(attr)


def _main(argv):
    if len(argv) < 2:
        exit("Usage: ph command [args]\n       ph help")
    cmd = argv[1]
    if cmd in ("-v", "--version"):
        print_version()
        exit()
    if cmd not in COMMANDS:
        exit("Unknown command {}.".format(cmd))

    # Self-implemented parsing of arguments.
    # Arguments of type "abc" and "--abc" go into args
    # Arguments of type "--abc=def" go into kwargs as key, value pairs
    args = []
    kwarg = {}
    for a in argv[2:]:
        if KWARG.match(a):
            args.append(a)
        elif KWARG_WITH_VALUE.match(a):
            split = a.index("=")
            k = a[2:split]
            v = a[split + 1 :]
            kwarg[k] = v
        else:
            args.append(__tryparse(a))
    try:
        COMMANDS[cmd](*args, **kwarg)
    except TypeError as err:
        exit(err)


def main():
    _main(sys.argv)


if __name__ == "__main__":
    main()
