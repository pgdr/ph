#!/usr/bin/env python
from __future__ import print_function
from .tabulate import tabulate as tabulate_
import sys
import pandas as pd
import re
import datetime


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
DOCS = {}


def _gpx(fname):
    try:
        import gpxpy
    except ImportError:
        sys.exit("ph gpx needs gpxpy, pip install ph[gpx]")

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
    "tsv": _tsv,
    "gpx": _gpx,
}

try:
    READERS["excel"] = pd.read_excel
    READERS["xls"] = pd.read_excel
    READERS["odf"] = pd.read_excel
except AttributeError:
    pass

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


FALSY = ("False", "false", "No", "no", "0", False, 0, "None")
TRUTHY = ("True", "true", "Yes", "yes", "1", True, 1)


def _assert_col(df, col, caller=None):
    if col not in df.columns:
        if caller is not None:
            sys.exit("ph {}: Unknown column {}".format(caller, col))
        sys.exit("Unknown column {}".format(col))


def _assert_cols(df, cols, caller=None):
    for col in cols:
        _assert_col(df, col, caller=caller)


def register(fn, name=None):
    if name is None:
        name = fn.__name__
    COMMANDS[name] = fn
    DOCS[name] = fn.__doc__
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
        sys.exit("You need scikit-learn.  Install ph[data].")

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
        sys.exit()

    if dset not in TOYDATA.keys() | REALDATA.keys():
        sys.exit("Unknown dataset {}.  See ph help dataset.".format(dset))
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
def diff(*cols, periods=1, axis=0):
    """Calculate the difference of an element compared with another element
    in the csv file (default is element in previous row).

    Argument: --periods=1

    Periods to shift for calculating difference, default 1.  Accepts
    negative values.

    Argument: --axis=0

    Take difference over rows (0) or columns (1), default 0.

    """

    df = pipein()
    if not cols:
        df = df.diff(periods=periods, axis=axis)
    else:
        _assert_cols(df, cols, "diff")
        columns = list(cols)
        df[columns] = df[columns].diff(periods=periods, axis=axis)
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
            sys.exit("ph dropna --axis=0 or --axis=1, not {}".format(axis))
    except ValueError:
        sys.exit("ph dropna --axis=0 or --axis=1, not {}".format(axis))

    if thresh is not None:
        try:
            thresh = int(thresh)
        except ValueError:
            sys.exit("ph dropna --thresh=0 or --thresh=1, not {}".format(thresh))

    df = pipein()
    try:
        df = df.dropna(axis=axis, how=how, thresh=thresh)
    except Exception as err:
        sys.exit(str(err))
    pipeout(df)


def _safe_out(output):
    """Prints output to standard out, catching broken pipe."""
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


def pipeout(df, sep=",", index=False, *args, **kwargs):
    csv = df.to_csv(sep=sep, index=index, *args, **kwargs)
    output = csv.rstrip("\n")
    _safe_out(output)


def pipein(ftype="csv", **kwargs):
    skiprows = kwargs.get("skiprows")
    if skiprows is not None:
        try:
            skiprows = int(skiprows)
            if skiprows < 0:
                raise ValueError("Negative")
        except ValueError:
            sys.exit("skiprows must be a non-negative int, not {}".format(skiprows))
        kwargs["skiprows"] = skiprows

    if kwargs.get("sep") == "\\t":
        kwargs["sep"] = "\t"

    try:
        return READERS[ftype](sys.stdin, **kwargs)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    except pd.errors.ParserError as err:
        sys.exit(str(err))


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
            sys.exit("--limit=x must be an integer, not {}".format(limit))
    METHODS = ("backfill", "bfill", "pad", "ffill")
    if method is not None:
        if method not in METHODS:
            sys.exit("method must be one of {}, not {}".format(METHODS, method))
        pipeout(pipein().fillna(method=method, limit=limit))
    elif value is not None:
        value = __tryparse(value)
        pipeout(pipein().fillna(value=value, limit=limit))
    else:
        sys.exit("'ph fillna' needs exactly one of value and method")


@register
def query(expr):
    """Using pandas queries.

    Usage: cat a.csv | ph query "x > 5"

    """
    df = pipein()
    new_df = df.query(expr)
    pipeout(new_df)


@register
def grep(*expr, case=True, na=float("nan"), regex=True, column=None):
    """Grep (with regex) for content in csv file.

    Usage: cat a.csv | ph grep 0
           cat a.csv | ph grep search_string
           cat a.csv | ph grep "A|B"               # search hits a or b
           cat a.csv | ph grep "a|b" --case=False  # case insensitive
           cat a.csv | ph grep 4 --column=x

    To disable regex (e.g. simple search for "." or "*" characters, use
    --regex=False).

    Search only in a specific column with --column=col.

    Supports regex search queries such as "0-9A-F" and "\\d" (possibly
    double-escaped.)

    """
    df = pipein()

    if case is True or case in TRUTHY:
        case = True
    elif case in FALSY:
        case = False
    else:
        sys.exit("ph grep:  Unknown --case={} should be True or False".format(case))

    if regex is True or regex in TRUTHY:
        regex = True
    elif regex in FALSY:
        regex = False
    else:
        sys.exit("ph grep:  Unknown --regex={} should be True or False".format(regex))

    if column is not None:
        _assert_col(df, column, "grep")

    expr = " ".join(str(e) for e in expr)  # force string input

    try:
        import numpy
    except ImportError:
        sys.exit("numpy needed for grep.  pip install numpy")

    retval = df[
        numpy.logical_or.reduce(
            [
                df[col].astype(str).str.contains(expr, case=case, na=na, regex=regex)
                for col in (df.columns if column is None else [column])
            ]
        )
    ]
    pipeout(retval)


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
def split(col, pat=" "):
    """Split a column in two based on a given pattern, default is " ".

    The resulting csv will have one extra column called "col_rhs" where
    "col" is the name of the column being split.

    Usage: cat dates.csv | ph split date /

    """
    pat = str(pat)
    df = pipein()
    _assert_col(df, col, "split")
    new_name = col + "_rhs"
    suffix = ""
    name = lambda: (new_name + "_" + str(suffix)).rstrip("_")
    while name() in df.columns:
        if not suffix:
            suffix = 1
        suffix += 1
    df[[col, name()]] = df[col].astype(str).str.split(pat=pat, n=1, expand=True)
    pipeout(df)


@register
def strip(*cols, lstrip=False, rstrip=False):
    """Strip (trim) a string.

    Usage: cat x.csv | ph strip
           cat x.csv | ph strip --lstrip=True
           cat x.csv | ph strip --rstrip=True

    """
    df = pipein()
    if not cols:
        cols = list(df.columns)
    else:
        cols = list(cols)
    _assert_cols(df, cols, "strip")
    for c in cols:
        if lstrip in TRUTHY:
            df[c] = df[c].str.lstrip()
        elif rstrip in TRUTHY:
            df[c] = df[c].str.rstrip()
        else:
            df[c] = df[c].str.strip()
    pipeout(df)


@register
def removeprefix(col, prefix=" "):
    """Remove prefix of contents of a column.

    Usage: cat a.csv | ph removeprefix col1 ..

    See also @removesuffix @strip

    """
    prefix = str(prefix)
    plen = len(prefix)
    df = pipein()
    _assert_col(df, col, "removeprefix")
    df[col] = df[col].apply(
        lambda s: str(s)[plen:] if str(s).startswith(prefix) else str(s)
    )
    pipeout(df)


@register
def removesuffix(col, suffix=" "):
    """Remove suffix of contents of a column.

    Usage: cat a.csv | ph removesuffix col1 ..

    See also @removeprefix @strip

    """
    suffix = str(suffix)
    plen = len(suffix)
    df = pipein()
    _assert_col(df, col, "removesuffix")
    df[col] = df[col].apply(
        lambda s: str(s)[:-plen] if str(s).endswith(suffix) else str(s)
    )
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
        sys.exit("Could not convert to {}: {}".format(type, err))
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
def groupby(*columns, how="sum", as_index=False):
    """Group by columns, then apply `how` function.

    Usage: cat a.csv | ph groupby animal  # default to sum
           cat a.csv | ph groupby animal --how=mean
           cat a.csv | ph groupby animal --how=prod
           cat a.csv | ph groupby animal --as_index=True  # removes index
    """
    columns = list(columns)
    if not columns:
        sys.exit("Needs at least one column to group by")
    df = pipein()
    _assert_cols(df, columns, "groupby")
    if as_index in TRUTHY:
        as_index = True
    elif as_index in FALSY:
        as_index = False
    else:
        sys.exit("--as_index=True or False, not {}".format(as_index))

    grouped = df.groupby(columns, as_index=as_index)
    try:
        fn = getattr(grouped, how)
    except AttributeError:
        sys.exit("Unknown --how={}, should be sum, mean, ...".format(how))
    retval = fn()

    pipeout(retval)


@register
def rolling(window, *columns, how="sum", win_type=None, std=None, beta=None, tau=None):
    """Rolling window calculations using provided `how` function.

    Usage: cat a.csv | ph rolling 3
           cat a.csv | ph rolling 5 --how=mean
           cat a.csv | ph rolling 5 colA colB --how=mean
           cat a.csv | ph rolling 5 --win_type=gaussian --std=7.62
    """
    df = pipein()
    orig_columns = list(df.columns)
    columns = list(columns)
    _assert_cols(df, columns, "rolling")

    if not columns:
        columns = list(df.columns)

    noncols = [c for c in df.columns if c not in columns]

    rollin = df[columns].rolling(window, win_type=win_type)
    nonrollin = df[noncols]
    try:
        fn = getattr(rollin, how)
    except AttributeError:
        sys.exit("Unknown --how={}, should be sum, mean, ...".format(how))

    if {std, beta, tau} != {None}:
        retval = fn(std=std, beta=beta, tau=tau)
    else:
        retval = fn()

    df = pd.concat([retval, nonrollin], axis=1)
    for col in orig_columns:
        if col not in df.columns:
            op = "ph rolling"
            sys.exit(
                '{}: Could not perform rolling window on column "{}"'.format(op, col)
            )
    df = df[orig_columns]
    pipeout(df)


@register
def ewm(
    min_periods=0,
    adjust=True,
    ignore_na=False,
    axis=0,
    com=None,
    span=None,
    halflife=None,
    alpha=None,
    how="mean",
):
    """Provide exponential weighted functions.

    A related set of functions are exponentially weighted versions of
    several of the above statistics. A similar interface to rolling and
    expanding is accessed through the ewm method to receive an EWM
    object.  A number of expanding EW (exponentially weighted) methods
    are provided:

      * mean
      * var
      * std
      * corr
      * cov

    Usage: cat a.csv | ph ewm --com=0.5 --how=mean
           cat a.csv | ph ewm --halflife=0.5 --how=std

    """
    if {com, span, halflife, alpha} == {None}:
        sys.exit("Must pass one of com, span, halflife, or alpha")

    df = pipein()

    ewm_ = df.ewm(
        min_periods=min_periods,
        adjust=adjust,
        ignore_na=ignore_na,
        axis=axis,
        com=com,
        span=span,
        halflife=halflife,
        alpha=alpha,
    )
    try:
        fn = getattr(ewm_, how)
    except AttributeError:
        sys.exit("Unknown --how={}, should be mean, var, std, corr, cov..".format(how))

    retval = fn()

    pipeout(retval)


@register
def expanding(min_periods=1, axis=0, how="sum", quantile=None):
    """Provide expanding transformations.

    A common alternative to rolling statistics is to use an expanding
    window, which yields the value of the statistic with all the data
    available up to that point in time.

    For working with data, a number of window functions are provided for
    computing common window or rolling statistics.  Among these are
    count, sum, mean, median, correlation, variance, covariance,
    standard deviation, skewness, and kurtosis.


    Usage: cat a.csv | ph expanding
           cat a.csv | ph expanding 1 --how=sum   # above equivalent to this
           cat a.csv | ph expanding 2
           cat a.csv | ph expanding 5 --how=quantile --quantile=0.25

    """

    df = pipein()

    if quantile is not None:
        if how != "quantile":
            sys.exit("Use both or none of --how=quantile and --quantile=<float>")
    if how == "quantile" and quantile is None:

        sys.exit("--how=quantile needs --quantile=<float>, e.g. --quantile=0.25")
    expanding_ = df.expanding(min_periods=min_periods, axis=axis)
    try:
        fn = getattr(expanding_, how)
    except AttributeError:
        sys.exit("Unknown --how={}, should be sum, mean, max, quantile..".format(how))

    if how == "quantile":
        retval = fn(quantile)
    else:
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
        sys.exit("Unknown column {}".format(column))
    if direction not in "+-":
        sys.exit("direction must be either + or -")
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
           cat a.csv | ph iplot --kind=scatter --mode=markers --x=x --y=y


    Depends on cufflinks: pip install ph[iplot].

    """
    try:
        import cufflinks  # noqa
        import plotly as py
    except ImportError:
        sys.exit("iplot needs cufflinks, pip install ph[iplot]")

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
            ph plot --savefig=fname.png
            ph plot --savefig=fname.svg
            ph plot --savefig=fname.svg --savefig-dpi=300
    """
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        sys.exit("plot depends on matplotlib, install ph[plot]")

    df = pipein()
    index = kwargs.get("index")
    if index is not None:
        _assert_col(df, index, caller="plot")
        df = df.set_index(index)
        del kwargs["index"]
    for log_ in ("logx", "logy", "loglog"):
        if kwargs.get(log_) in TRUTHY:
            kwargs[log_] = True
    fname = kwargs.get("savefig")
    dpi = kwargs.get("savefig-dpi")

    if fname:
        del kwargs["savefig"]
    if dpi:
        del kwargs["savefig-dpi"]

    fig, ax = plt.subplots()
    df.plot(**kwargs, ax=ax)

    if index == "date":
        fig.autofmt_xdate()

    if fname:
        plt.tight_layout()
        plt.savefig(fname, dpi=dpi)
    else:
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
def date(col=None, unit=None, origin="unix", errors="raise", dayfirst=False, **kwargs):
    """Assemble datetime from multiple columns or from one column

    --unit can be D, s, us, ns (defaults to ns, ns from origin)

    --origin can be unix, julian, or time offset, e.g. '2000-01-01'

    --errors can be raise, coerce, ignore (see pandas.to_datetime)

    --format a strptime format string, e.g. '%Y-%m-%d %H:%M:%S'

    --utc=True if the input is in utc, i.e. seconds from epoch

    Usage: cat a.csv | ph date x
           cat a.csv | ph date x --unit=s --origin="1984-05-17 09:30"
           cat a.csv | ph date x --dayfirst=True
           cat a.csv | ph date  # if a.csv contains year, month, date
           cat a.csv | ph date x --format="%Y-%m-%d"
           cat a.csv | ph date x --utc=True

    """
    DATE_ERRORS = ("ignore", "raise", "coerce")
    if errors not in DATE_ERRORS:
        sys.exit("Errors must be one of {}, not {}.".format(DATE_ERRORS, errors))

    dayfirst = dayfirst in TRUTHY

    date_parser = None
    if "format" in kwargs:
        date_parser = lambda d: [
            datetime.datetime.strptime(str(e), kwargs["format"]) for e in d
        ]
    if kwargs.get("utc") in TRUTHY:
        date_parser = lambda d: [datetime.datetime.utcfromtimestamp(e) for e in d]
    df = pipein()
    try:
        if col is None:
            df = pd.to_datetime(df, unit=unit, origin=origin, errors=errors)
        else:
            _assert_col(df, col, "date")
            if date_parser is None:
                df[col] = pd.to_datetime(
                    df[col], unit=unit, origin=origin, errors=errors, dayfirst=dayfirst
                )
            else:
                df[col] = date_parser(df[col])
    except Exception as err:
        sys.exit(err)

    pipeout(df)


@register
def describe():
    """Run DataFrame's describe method.

    The result is NOT tabular data, so pipeline ends.

    Usage: cat a.csv | ph describe
    """
    df = pipein()
    try:
        out = df.describe()
    except ValueError as err:
        sys.exit(str(err))
    _safe_out(out)


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
        sys.exit("Unknown datatype {}.".format(ftype))

    if not fname:
        if ftype in ("parquet", "xls", "xlsx", "ods", "pickle"):
            sys.exit("{} needs a path".format(ftype))

    if ftype == "hdf5":
        sys.exit("hdf5 writer not implemented")

    if index not in TRUTHY + FALSY:
        sys.exit("Index must be True or False, not {}".format(index))
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
        sys.exit()

    if sep is not None:
        if ftype != "csv":
            sys.exit("Only csv mode supports separator")

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
           cat /etc/passwd | ph from csv --sep=':' --header=None

    The following pipes should be equivalent:

    cat a.csv
    cat a.csv | ph to json | ph from json
    cat a.tsv | ph from tsv
    cat a.tsv | ph from csv --sep='\t'
    cat a.tsv | ph from csv --sep='\t' --thousands=','

    In the event that the csv data starts on the first line (i.e. no
    header is present), use --header=None.
    """
    if "header" in kwargs:
        kwargs["header"] = __tryparse(kwargs["header"])
    skiprows = kwargs.get("skiprows")
    if skiprows is not None:
        try:
            skiprows = int(skiprows)
            if skiprows < 0:
                raise ValueError("Negative")
        except ValueError:
            sys.exit("skiprows must be a non-negative int, not {}".format(skiprows))
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
        sys.exit("Unknown axis command '{}'".format(axis))
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
def merge(fname1, fname2, how="inner", on=None, left=None, right=None):
    """Merging two csv files.

    If the two files have a common column name, then the merge will be
    on that column.  If the files have several common column names, use
    --on=key for merging on a specific column.

    If you want to merge on columns with different names, use
    --left=lkey --right=rkey.

    Choose between left merge, right merge, inner merge and outer merge
    by using (e.g.) --how=inner.

    Usage: ph merge a.csv b.csv --on=ijk
           ph merge a.csv b.csv --on ijk --how=inner
           ph merge a.csv b.csv --left=key_a --right=key_b

    """
    hows = ("left", "right", "outer", "inner")
    if how not in hows:
        sys.exit("Unknown merge --how={}, must be one of {}".format(how, hows))
    try:
        df1 = pd.read_csv(fname1)
        df2 = pd.read_csv(fname2)
    except Exception as err:
        sys.exit(str(err))
    if set([on, left, right]) == set([None]) and not set(df1.columns).intersection(set(df2.columns)):
        sys.exit("No common columns to perform merge on.  Merge options: on, or: left=None, right=None.")
    if set([on, left, right]) == set([None]):
        pipeout(pd.merge(df1, df2, how=how))
    else:
        if left is None and right is None:
            pipeout(pd.merge(df1, df2, how=how, on=on))
        elif left is not None and right is not None:
            _assert_col(df1, left, "merge")
            _assert_col(df2, right, "merge")
            pipeout(pd.merge(df1, df2, how=how, left_on=left, right_on=right))
        else:
            sys.exit("Specify columns in both files.  left was {}, right was {}".format(left, right))


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
    df = pipein()
    out = tabulate_(df, tablefmt=fmt, headers=headers, showindex=index)
    _safe_out(out)


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


@registerx("help")
def help_(*args, **kwargs):
    """Writes help (docstring) about the different commands."""
    if not args:
        print("Usage: ph command arguments")
        print(USAGE_TEXT)
        _print_commands(sorted(COMMANDS.keys()))
        sys.exit(0)
    cmd = args[0]

    ds = None
    if cmd in DOCS:
        ds = DOCS[cmd]
    else:
        try:
            fn = getattr(pd.DataFrame, cmd)
            ds = getattr(fn, "__doc__")
        except AttributeError:
            pass
    if ds is None:
        sys.exit("Unknown command {}".format(cmd))
    print("Usage: ph {}".format(cmd))
    print("       {}".format(ds.strip()))


def slugify_name(name):
    name_ = name
    try:
        name = float(name_)
    except ValueError:
        pass
    if isinstance(name_, (int, str)):
        try:
            name = int(name_)
        except ValueError:
            pass
    if isinstance(name, (int, float)):
        name = str(name) + "_"
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


@register
def raw(fname=None):
    """Do your best to read this comma-separated input."""
    import csv

    if fname is None:
        d = csv.reader(sys.stdin)
        df = pd.DataFrame(d)
    else:
        with open(fname, "r") as fin:
            d = csv.reader(fin)
            df = pd.DataFrame(d)
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
           ph open excel a.xls --sheet_name=2
           ph open excel a.xls --sheet_name="The Real Dataset sheet"
           ph open csv a.csv --thousands=','


    In the event that the csv data starts on the first line (i.e. no
    header is present), use --header=None.

    """
    if "header" in kwargs:
        kwargs["header"] = __tryparse(kwargs["header"])

    if ftype not in READERS:
        sys.exit("Unknown filetype {}".format(ftype))
    reader = READERS[ftype]

    if kwargs.get("sep") == "\\t":
        kwargs["sep"] = "\t"

    if ftype == "clipboard" and fname is not None:
        sys.exit("clipboard does not take fname")
    if ftype != "clipboard" and fname is None:
        sys.exit("filename is required for {}".format(ftype))

    skiprows = kwargs.get("skiprows")
    if skiprows is not None:
        try:
            skiprows = int(skiprows)
            if skiprows < 0:
                raise ValueError("Negative")
        except ValueError:
            sys.exit("skiprows must be a non-negative int, not {}".format(skiprows))
        kwargs["skiprows"] = skiprows

    try:
        if ftype == "clipboard":
            df = reader(**kwargs)
        elif ftype in ("excel", "xls", "odf"):
            try:
                df = reader(fname, **kwargs)
            except Exception as err:
                sys.exit(err)
            if not isinstance(df, pd.DataFrame):  # could be dict
                try:
                    errormsg = 'Specify --sheet_name="{}"'.format(
                        "|".join(str(k) for k in df.keys())
                    )
                except Exception:
                    errormsg = "Specify --sheet_name"
                sys.exit(errormsg)
        else:
            df = reader(fname, **kwargs)
    except AttributeError as err:
        sys.exit(
            "{} is not supported in your Pandas installation\n{}".format(ftype, err)
        )
    except ImportError as err:
        sys.exit(
            "{} is not supported in your Pandas installation\n{}".format(ftype, err)
        )
    except FileNotFoundError as err:
        sys.exit("File not found: {}".format(err))
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
    def partial(*args, **kwargs):
        return _call(attr, *args, **kwargs)

    partial.__name__ = attr
    COMMANDS[attr] = partial


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
    if x is None or x == "None":
        return None
    x_ = x
    try:
        x_ = float(x)
        x_ = int(x)
    except ValueError:
        pass
    except OverflowError:
        x_ = float("inf")
    return x_


@register
def replace(old, new, column=None, newcolumn=None):
    """Replace a value (in a column) with a new value.

    Usage: cat a.csv | ph replace 8 100 # replace in all columns
           cat a.csv | ph replace 8 100 --column=y
           cat a.csv | ph replace 8 100 --column=y --newcolumn=z

    Beware that it is difficult to know which _types_ we are searching for,
    therefore we only apply a heuristic, which is doomed to be faulty.
    """
    if newcolumn is None:
        newcolumn = column
    df = pipein()

    if column is None:
        if newcolumn is not None:
            sys.exit("Cannot use newcolumn and not column.")
        df = df.replace(to_replace=old, value=new, inplace=False)
    elif column not in df:
        sys.exit("Column {} does not exist.".format(column))
    else:
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

    _assert_cols(df, cols, "columns")

    if not cols and not kwargs:
        print("columns")
        print("\n".join(list(df.columns)))
    else:
        pipeout(df[cols])


@register
def spencer(*cols):
    """Compute Spencer's 15-weight average.

    Usage: cat a.csv | ph spencer

    Experimental feature for computing Spencer's 15-weight average.
    Smooths out curves by removing high frequency noise.  Will
    ultimately lose some data on each end of the timeseries.

    """
    _SPENCER = (-3, -6, -5, 3, 21, 46, 67, 74, 67, 46, 21, 3, -5, -6, -3)
    _SPENCER_SUM = sum(_SPENCER)

    def spencer_(lst):
        for i in range(7, len(lst) - 8):
            seq = lst[i - 7 : i + 8]
            yield sum(seq[i] * _SPENCER[i] / _SPENCER_SUM for i in range(15))

    df = pipein()
    _assert_cols(df, cols, "spencer")
    prefix = [float("nan")] * 7
    suffix = [float("nan")] * 8
    if not cols:
        cols = list(df.columns)
    for col in cols:
        lst = list(df[col])
        s = list(spencer_(lst))
        ncol = prefix + s + suffix
        df[col] = ncol
    pipeout(df)


def _parse_slice(slicestr):
    pattern = ":<int> | <int>: | <int>:<int> | <int>:<int>:<int>"
    error = "Input to slice is {} _not_ {}".format(pattern, slicestr)

    assert ":" in slicestr, error
    start = None
    end = None
    step = None
    tup = slicestr.split(":")
    if len(tup) > 3:
        sys.exit(error)
    start = tup[0] or None
    if start is not None:
        start = int(start)
    end = tup[1] or None
    if end is not None:
        end = int(end)
    if len(tup) == 3:
        step = tup[2] or None
        if step is not None:
            step = int(step)
    return start, end, step


@registerx("slice")
def slice_(slicestr):
    """Slice a dataframe with Python slice pattern.

    Usage: cat a.csv | ph slice :10    # head
           cat a.csv | ph slice -10:   # tail
           cat a.csv | ph slice ::2    # every even row
           cat a.csv | ph slice 1::2   # every odd row
           cat a.csv | ph slice ::-1   # reverse file

    """
    pattern = ":<int> | <int>: | <int>:<int> | <int>:<int>:<int>"
    error = "Input to slice is {} _not_ {}".format(pattern, slicestr)
    df = pipein()
    if isinstance(slicestr, int) or ":" not in slicestr:
        sys.exit(error)
    start, end, step = _parse_slice(slicestr)
    retval = df[start:end:step]
    pipeout(retval)


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
        sys.exit("inplace is nonsensical in ph")

    df = pipein()

    if kwargs.get("axis") in (None, 0, "index"):
        columns = [__tryparse(col) for col in columns]
    elif kwargs.get("axis") in (1, "columns"):
        _assert_cols(df, columns, "drop")
    else:
        sys.exit(
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
    _assert_col(df, col, "sort")
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
    _assert_cols(df, (x, y), "polyfit")
    deg = __tryparse(deg)
    if not isinstance(deg, int) or deg <= 0:
        sys.exit("deg={} should be a positive int".format(deg))
    try:
        import numpy
    except ImportError:
        sys.exit("numpy needed for polyfit.  pip install numpy")

    polynomial = numpy.polynomial.Polynomial.fit(df[x], df[y], deg=deg)
    df["polyfit_{}".format(deg)] = df[x].map(polynomial)
    pipeout(df)


def __process(attr):
    if attr in COMMANDS:
        return False
    if attr.startswith("_"):
        return False
    if attr.startswith("to_"):
        return False
    if attr == "T":
        return False
    return True


for attr in dir(pd.DataFrame):
    if __process(attr):
        register_forward(attr)


def _main(argv):
    if len(argv) < 2:
        sys.exit("Usage: ph command [args]\n       ph help")
    cmd = argv[1]
    if cmd in ("-v", "--version"):
        print_version()
        sys.exit()
    if cmd in ("-h", "--h", "--help"):
        cmd = "help"
    if cmd not in COMMANDS:
        sys.exit("Unknown command {}.".format(cmd))

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
            kwarg[k] = __tryparse(v)
        else:
            args.append(__tryparse(a))
    try:
        COMMANDS[cmd](*args, **kwarg)
    except TypeError as err:
        sys.exit(err)


def main():
    _main(sys.argv)


if __name__ == "__main__":
    main()
