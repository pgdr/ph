import ph

import os.path
import io

import pytest
import contextlib

import pandas as pd
import datetime as dt
import math

NAN = float("nan")


def __have_xlrd():
    try:
        import xlrd  # noqa

        return True
    except ImportError:
        return False


def _assert_a(df):
    assert list(df.shape) == [6, 2]
    assert list(df.columns) == ["x", "y"]
    assert list(df["x"]) == list(range(3, 9))
    assert list(df["y"]) == list(range(8, 14))


def _get_path(name, extension="csv"):
    pth = "test_data/{}.{}".format(name, extension)
    root = os.path.dirname(__file__)
    path = os.path.abspath(os.path.join(root, pth))

    return path


def _get_data(name, extension="csv"):
    path = _get_path(name, extension)
    with open(path, "r") as fin:
        data = "".join(fin.readlines())

    return data


def _get_io(name, extension="csv"):
    return io.StringIO(_get_data(name, extension))


class Capture:
    # Just a mutable string container for ctx mgr around capture.out
    def __init__(self, outerr=None):
        if outerr is not None:
            self.out = outerr.out
            self.err = outerr.err
        else:
            self.out = ""
            self.err = ""
        self._df = None

    @property
    def df(self):
        if self._df is None:
            self._df = pd.read_csv(io.StringIO(self.out))
        return self._df

    def read_df(self, *args, **kwargs):
        self._df = pd.read_csv(io.StringIO(self.out), *args, **kwargs)
        return self.df

    def assert_shape(self, rows, cols):
        assert list(self.df.shape) == [rows, cols]

    def assert_columns(self, columns):
        assert list(self.df.columns) == list(columns)


@pytest.fixture
def phmgr(capsys, monkeypatch):
    @contextlib.contextmanager
    def phmgr(dataset="a", extension="csv"):
        monkeypatch.setattr("sys.stdin", _get_io(dataset, extension))
        cap = Capture()
        yield cap
        outerr = capsys.readouterr()
        cap.out, cap.err = outerr.out, outerr.err
        assert not cap.err, "Std error not empty: {}".format(cap.err)

    return phmgr


def _call(cmd, extra=None):
    if extra is None:
        extra = []
    ph._main(["ph"] + cmd.split(" ") + extra)


def test_cat(phmgr):
    with phmgr() as captured:
        _call("cat")
    assert captured.out == _get_data("a")


def test_cat_many(capsys):
    _call("cat {} {} --axis=index".format(_get_path("a"), _get_path("covid")))
    cap = Capture(capsys.readouterr())
    assert not cap.err
    cap.assert_shape(35, 12)

    _call("cat {} {} --axis=columns".format(_get_path("a"), _get_path("covid")))
    cap = Capture(capsys.readouterr())
    assert not cap.err
    cap.assert_shape(29, 12)


def test_columns(phmgr):
    with phmgr("iris") as captured:
        _call("columns")
    assert not captured.err
    captured.assert_columns(["columns"])
    assert list(captured.df["columns"]) == [
        "150",
        "4",
        "setosa",
        "versicolor",
        "virginica",
    ]


def test_columns_args(phmgr):
    with phmgr("iris") as captured:
        _call("columns setosa versicolor")
    assert not captured.err
    captured.assert_shape(150, 2)
    captured.assert_columns(["setosa", "versicolor"])


def test_drop_columns(phmgr):
    with phmgr("iris") as captured:
        _call("drop setosa virginica --axis=columns")
    assert not captured.err
    df = captured.df
    captured.assert_shape(150, 3)
    captured.assert_columns(
        ["150", "4", "versicolor",]
    )
    assert list(df.iloc[0]) == [5.1, 3.5, 0.2]


def test_drop_index(phmgr):
    with phmgr("iris") as captured:
        _call("drop 0 --axis=index")
    assert not captured.err
    df = captured.df
    captured.assert_shape(149, 5)
    assert list(df.iloc[0]) == [4.9, 3.0, 1.4, 0.2, 0]


def test_open_skiprows(capsys):
    _call("open csv {} --skiprows=6".format(_get_path("f")))
    captured = Capture(capsys.readouterr())
    assert not captured.err
    df = captured.df
    captured.assert_shape(2, 2)
    assert list(df.iloc[0]) == [14, 13]
    assert list(df.iloc[1]) == [16, 21]


def test_from_headless(phmgr):
    with phmgr("headless") as captured:
        _call("from --header=None")
    assert not captured.err
    captured.assert_shape(5, 2)
    captured.assert_columns(["0", "1"])


def test_open_headless(capsys):
    _call("open csv {} --header=None".format(_get_path("headless")))
    captured = Capture(capsys.readouterr())
    assert not captured.err
    captured.assert_shape(5, 2)
    captured.assert_columns(["0", "1"])


def test_diff_all(phmgr):
    with phmgr() as captured:
        _call("diff --periods=2")
    assert not captured.err
    captured.assert_shape(6, 2)
    captured.assert_columns(["x", "y"])


def test_diff_xy(phmgr):
    with phmgr() as captured:
        _call("diff x y --periods=2")
    assert not captured.err


def test_diff_with_date_col(phmgr):
    with phmgr("usa") as captured:
        _call("diff cases deaths")
    assert not captured.err
    captured.assert_shape(93, 7)
    df = captured.df
    c = list(df["cases"])
    d = list(df["deaths"])
    assert c[1:6] == [-3403.0, -3235.0, 1619.0, -1284.0, -1898.0]
    assert d[1:7] == [-248.0, -343.0, 166.0, -73.0, -165.0, 3.0]


@pytest.mark.skipif(
    os.getenv("GITHUB_WORKFLOW") is not None, reason="clipboard not on headless"
)
def test_clipboard(capsys):
    # This test is a bit nasty as we necessarily need to modify the
    # clipboard.  We do, however, try to preserve the content.  YMMV.
    import pandas.io.clipboard as cp

    old = cp.paste()
    try:
        df = pd.read_csv(_get_path("a"))
        df.to_clipboard()

        _call("from clipboard")
        captured = Capture(capsys.readouterr())
        assert not captured.err
        df = captured.df
        captured.assert_shape(6, 2)
    finally:
        cp.copy(old)


def test_sep_from(phmgr):
    with phmgr("d", extension="scsv") as captured:
        _call("from csv --sep=;")
    assert not captured.err
    captured.assert_shape(6, 3)


def test_from_skiprows(phmgr):
    with phmgr("f") as captured:
        _call("from csv --skiprows=6")
    assert not captured.err
    df = captured.df
    captured.assert_shape(2, 2)
    assert list(df.iloc[0]) == [14, 13]
    assert list(df.iloc[1]) == [16, 21]


def test_sep_to_with_sep(capsys, monkeypatch):
    monkeypatch.setattr("sys.stdin", _get_io("d"))
    _call("to csv --sep=_")
    captured = Capture(capsys.readouterr())
    assert not captured.err
    captured.assert_shape(6, 1)

    df = pd.read_csv(io.StringIO(captured.out), sep="_")
    assert list(df.shape) == [6, 3]
    assert list(df["year"]) == list(range(2003, 2009))


def test_sep_to_with_index(capsys, monkeypatch):
    monkeypatch.setattr("sys.stdin", _get_io("d"))
    _call("to csv --index=true")
    captured = Capture(capsys.readouterr())
    assert not captured.err
    captured.assert_shape(6, 4)


def test_thousands_from(capsys, monkeypatch):
    monkeypatch.setattr("sys.stdin", _get_io("t", extension="tsv"))
    _call("from csv --thousands=, --sep=\t")
    captured = Capture(capsys.readouterr())
    assert not captured.err
    df = captured.df
    captured.assert_shape(7, 2)
    assert all(df["a"] == 10 ** df["b"])


def test_thousands_from_escaped_tab(capsys, monkeypatch):
    monkeypatch.setattr("sys.stdin", _get_io("t", extension="tsv"))
    _call("from csv --thousands=, --sep=\\t")
    captured = Capture(capsys.readouterr())
    assert not captured.err
    df = captured.df
    captured.assert_shape(7, 2)
    assert all(df["a"] == 10 ** df["b"])


def test_describe(phmgr):
    with phmgr() as captured:
        _call("describe")
    assert len(captured.out.split("\n")) == 10
    header = set(captured.out.split("\n")[0].split())
    assert "x" in header
    assert "y" in header
    assert "max" in captured.out


def test_shape(phmgr):
    with phmgr("covid") as captured:
        _call("shape")
    df = captured.df
    captured.assert_columns(["rows", "columns"])
    assert list(df["rows"]) == [29]
    assert list(df["columns"]) == [10]


def test_transpose(phmgr):
    with phmgr() as captured:
        _call("transpose")
    assert (
        captured.out
        == """\
0,1,2,3,4,5
3,4,5,6,7,8
8,9,10,11,12,13
"""
    )


def test_head_tail(capsys, monkeypatch):
    monkeypatch.setattr("sys.stdin", _get_io("a"))
    _call("head 7")
    captured = capsys.readouterr()
    assert not captured.err

    monkeypatch.setattr("sys.stdin", io.StringIO(captured.out))
    _call("tail 3")
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


def test_open_with_decimals(phmgr):
    with phmgr("padded_decimals") as captured:
        _call("from csv --decimal=, --thousands=.")
    assert not captured.err
    df = captured.df
    captured.assert_shape(7, 2)
    assert "paddecim" in df.columns
    assert str(df["paddecim"].dtype).startswith("float")
    assert df["paddecim"].sum() == 1470.0 * 2


def test_from_with_decimals(capsys, monkeypatch):
    monkeypatch.setattr("sys.stdin", _get_io("padded_decimals"))
    _call("from csv --decimal=, --thousands=.")
    captured = Capture(capsys.readouterr())

    assert not captured.err
    df = captured.df
    captured.assert_shape(7, 2)
    assert "paddecim" in df.columns
    assert str(df["paddecim"].dtype).startswith("float")
    assert df["paddecim"].sum() == 1470.0 * 2


def test_date(phmgr):
    with phmgr() as captured:
        _call("date x --unit=D")
    df = captured.df
    df["x"] = pd.to_datetime(captured.df["x"])
    assert list(df["y"]) == list(range(8, 14))
    x = list(df["x"])
    assert len(list(df["x"])) == 6
    for i in range(6):
        assert str(x[i]) == "1970-01-0{} 00:00:00".format(i + 4)

    with phmgr("d") as captured:
        _call("date")
    df = captured.df
    assert len(df) == 6
    captured.assert_columns(["0"])
    act = [str(x) for x in df["0"]]
    exp = [
        "2003-03-08",
        "2004-04-09",
        "2005-05-10",
        "2006-06-11",
        "2007-07-12",
        "2008-08-13",
    ]
    assert act == exp


def test_date_dayfirst(phmgr):
    with phmgr("usa") as captured:
        _call("date dateRep --dayfirst=True")
    df = captured.df
    captured.assert_shape(93, 7)
    df["dateRep"] = pd.to_datetime(df["dateRep"])
    df["realdate"] = pd.to_datetime(df[["year", "month", "day"]])
    assert all(df["realdate"] == df["dateRep"])


def test_date_errors(phmgr):
    with pytest.raises(SystemExit) as exit_:
        with phmgr("derr") as captured:
            _call("date --col=x")
    assert str(exit_.value) == "Unknown column x"

    with pytest.raises(SystemExit) as exit_:
        with phmgr("derr") as captured:
            _call("date --col=year")
    assert str(exit_.value).startswith("Out of bounds nanosecond timestamp")

    with pytest.raises(SystemExit) as exit_:
        with phmgr("derr") as captured:
            _call("date --col=year --errors=nosucherr")
    assert str(exit_.value).startswith("Errors must be one of")

    with phmgr("derr") as captured:
        _call("date --col=year --errors=coerce")
    assert not captured.err
    df = captured.df
    assert df["year"].dtype == dt.datetime

    with phmgr("derr") as captured:
        _call("date --col=year --errors=ignore")
    assert not captured.err
    df = captured.df
    assert "200-01" in list(df["year"])


def test_eval(phmgr):
    with phmgr() as captured:
        _call("eval", ["x = x**2"])
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


def test_dropna(phmgr):
    with phmgr("covid") as captured:
        _call("dropna")
    captured.assert_shape(5, 10)

    with phmgr("covid") as captured:
        _call("dropna --thresh=7")
    captured.assert_shape(15, 10)

    with phmgr("covid") as captured:
        _call("dropna --axis=1 --thresh=17")
    captured.assert_shape(29, 5)


def test_fillna(phmgr):
    with phmgr("covid") as captured:
        _call("fillna 17")
    assert captured.df["Canada"].sum() == 1401

    with phmgr("covid") as captured:
        _call("fillna 19 --limit=3")
    assert captured.df["Canada"].sum() == 1050

    with phmgr("covid") as captured:
        _call("fillna --method=pad --limit=5")
    assert captured.df["Canada"].sum() == 2493


def test_merge(capsys):
    lft = _get_path("left")
    rht = _get_path("right")
    ph.merge(lft, rht)
    cap = Capture(capsys.readouterr())
    assert not cap.err
    cap.assert_shape(3, 6)

    ph.merge(lft, rht, how="left")
    cap = Capture(capsys.readouterr())
    assert not cap.err
    cap.assert_shape(5, 6)

    ph.merge(lft, rht, how="outer")
    cap = Capture(capsys.readouterr())
    assert not cap.err
    cap.assert_shape(6, 6)

    ph.merge(lft, rht, on="key1")
    cap = Capture(capsys.readouterr())
    assert not cap.err
    cap.assert_shape(5, 7)


def test_groupby_sum_default(phmgr):
    with phmgr("group") as captured:
        _call("groupby Animal")
    assert not captured.err
    df = captured.df
    captured.assert_shape(2, 2)
    assert list(df.iloc[0]) == ["Falcon", 750.0]
    assert list(df.iloc[1]) == ["Parrot", 50.0]


def test_groupby_sum(phmgr):
    with phmgr("group") as captured:
        _call("groupby Animal --how=sum")
    assert not captured.err
    df = captured.df
    captured.assert_shape(2, 2)
    assert list(df.iloc[0]) == ["Falcon", 750.0]
    assert list(df.iloc[1]) == ["Parrot", 50.0]


def test_groupby_mean(phmgr):
    with phmgr("group") as captured:
        _call("groupby Animal --how=count --as_index=True")
    assert not captured.err
    df = captured.df
    captured.assert_shape(2, 1)
    assert list(df.iloc[0]) == [2]
    assert list(df.iloc[1]) == [2]


def test_groupby_first(phmgr):
    with phmgr("group") as captured:
        _call("groupby Animal --how=first")
    assert not captured.err
    df = captured.df
    captured.assert_shape(2, 2)
    assert list(df.iloc[0]) == ["Falcon", 380.0]
    assert list(df.iloc[1]) == ["Parrot", 24.0]


def test_rolling_default(phmgr):
    with phmgr("iris") as captured:
        _call("rolling 3")
    assert not captured.err
    captured.assert_shape(150, 5)
    assert captured.df["setosa"].dropna().sum() == pytest.approx(1671.0, 0.01)


def test_rolling_mean(phmgr):
    with phmgr("iris") as captured:
        _call("rolling 7 --how=mean")
    assert not captured.err
    captured.assert_shape(150, 5)
    assert captured.df["setosa"].dropna().sum() == pytest.approx(543.83, 0.01)


def test_ewm_default(phmgr):
    with phmgr("iris") as captured:
        _call("ewm 2 --com=0.5")
    assert not captured.err
    captured.assert_shape(150, 5)
    assert captured.df["setosa"].dropna().sum() == pytest.approx(560.411)


def test_expanding_default(phmgr):
    with phmgr("iris") as captured:
        _call("expanding 3")
    assert not captured.err
    captured.assert_shape(150, 5)
    assert captured.df["setosa"].dropna().sum() == pytest.approx(32468.9)


def test_expanding_quantile(phmgr):
    with phmgr("iris") as captured:
        _call("expanding 3 --how=quantile --quantile=0.9")
    assert not captured.err
    captured.assert_shape(150, 5)
    assert captured.df["setosa"].dropna().sum() == pytest.approx(563.81)


def test_index(phmgr):
    with phmgr("a") as captured:
        _call("index")

    assert not captured.err
    assert list(captured.df["index"]) == [i for i in range(6)]


def test_split(phmgr):
    with phmgr("padded_decimals") as captured:
        _call("split paddecim ,")
    assert not captured.err
    captured.assert_shape(7, 3)
    captured.assert_columns(["idx", "paddecim", "paddecim_rhs"])
    captured.read_df(thousands=".")
    assert set(captured.df["paddecim_rhs"]) == {0, 50}
    assert list(captured.df["paddecim"]) == [
        502,
        172,
        7,
        142,
        157,
        487,
        1470,
    ]


def test_sort(phmgr):
    with phmgr("iris") as captured:
        ph.sort("setosa")
    assert not captured.err
    lst = list(captured.df["setosa"])
    assert lst == sorted(lst)


def test_polyfit(phmgr):
    with phmgr() as captured:
        ph.polyfit("x", "y")
    assert not captured.err
    df = captured.df
    assert list(df.columns) == ["x", "y", "polyfit_1"]
    assert df["y"].equals(df["polyfit_1"].astype(int))


def test_version(phmgr):
    import ph._version

    with phmgr() as captured:
        ph.print_version()
    assert not captured.err
    assert captured.out == ph._version.__version__ + "\n"


def test_slugify_method():
    actexp = {
        "abc": "abc",
        "abc123": "abc123",
        "abc_ 123 ": "abc_123",
        "abc(123)": "abc_123",
        "abc(123)_": "abc_123_",
        "(abc)/123": "abc_123",
        "_abc: 123": "_abc_123",
        '[]()abc-^  \\ "': "abc",
    }
    for act, exp in actexp.items():
        assert ph.slugify_name(act) == exp


def test_replace(phmgr):
    with phmgr() as captured:
        _call("replace 8 100")
    assert not captured.err
    captured.assert_shape(6, 2)
    assert list(captured.df.x) == list(range(3, 8)) + [100]
    assert list(captured.df.y) == [100] + list(range(9, 14))


def test_replace_col_and_inf(phmgr):
    with phmgr("inf") as captured:
        _call("replace inf 0 --column=x")
    assert not captured.err
    captured.assert_shape(6, 2)

    x = captured.df.x
    lx = list(x)
    xna = x.dropna()
    assert list(xna) == [0, 7, 8]
    assert math.isnan(lx[0])
    assert math.isnan(lx[1])
    assert math.isnan(lx[2])
    assert list(captured.df.y) == list(range(8, 14))


def test_slice(phmgr):
    with phmgr("a") as captured:
        _call("slice 0:3")
    assert not captured.err
    captured.assert_shape(3, 2)
    assert list(captured.df.x) == list(range(3, 6))
    assert list(captured.df.y) == list(range(8, 11))


def test_slice_end(phmgr):
    with phmgr("a") as captured:
        _call("slice :3")
    assert not captured.err
    captured.assert_shape(3, 2)
    assert list(captured.df.x) == list(range(3, 6))
    assert list(captured.df.y) == list(range(8, 11))


def test_slice_start(phmgr):
    with phmgr("a") as captured:
        _call("slice 3:")
    assert not captured.err
    captured.assert_shape(3, 2)
    assert list(captured.df.x) == list(range(6, 9))
    assert list(captured.df.y) == list(range(11, 14))


def test_slice_start_step(phmgr):
    with phmgr("a") as captured:
        _call("slice 1::2")
    assert not captured.err
    captured.assert_shape(3, 2)
    assert list(captured.df.x) == list(range(4, 9, 2))
    assert list(captured.df.y) == list(range(9, 14, 2))


def test_slice_start_end_step(phmgr):
    with phmgr("a") as captured:
        _call("slice 1:5:2")
    assert not captured.err
    captured.assert_shape(2, 2)
    assert list(captured.df.x) == list(range(4, 7, 2))
    assert list(captured.df.y) == list(range(9, 12, 2))


def test_slugify_df(phmgr):
    with phmgr("slugit") as captured:
        _call("slugify")

    assert not captured.err

    cols = list(captured.df.columns)
    assert cols == ["stupid_column_1", "jerky_column_no_2"]


def test_doc_plot(capsys):
    _call("help plot")
    captured = Capture(capsys.readouterr())
    assert not captured.err
    assert "Plot the csv file" in captured.out


def test_median(phmgr):
    with phmgr() as captured:
        _call("median")
    assert not captured.err
    assert captured.out == "x,y\n5.5,10.5\n"


@pytest.mark.filterwarnings("ignore")
@pytest.mark.skipif(not __have_xlrd(), reason="missing xlrd")
def test_xlsx_default_sheet_0(capsys):
    pth = _get_path("sheet", extension="xlsx")
    cmd = "open excel {} {}".format(pth, "--skiprows=4")
    _call(cmd)
    captured = Capture(capsys.readouterr())
    assert not captured.err
    _assert_a(captured.df)


@pytest.mark.filterwarnings("ignore")
@pytest.mark.skipif(not __have_xlrd(), reason="missing xlrd")
def test_xlsx_explicit_sheet_0(capsys):
    pth = _get_path("sheet", extension="xlsx")
    cmd = "open excel {} {} {}".format(pth, "--skiprows=4", "--sheet_name=0")
    _call(cmd)
    captured = Capture(capsys.readouterr())
    assert not captured.err
    _assert_a(captured.df)


@pytest.mark.filterwarnings("ignore")
@pytest.mark.skipif(not __have_xlrd(), reason="missing xlrd")
def test_xlsx_sheet_1(capsys):
    pth = _get_path("sheet", extension="xlsx")
    cmd = "open excel {} {} {}".format(pth, "--skiprows=1", "--sheet_name=1")
    _call(cmd)
    captured = Capture(capsys.readouterr())
    assert not captured.err
    captured.assert_shape(6, 4)
    captured.assert_columns(["Unnamed: 0", "year", "month", "day"])


@pytest.mark.filterwarnings("ignore")
@pytest.mark.skipif(not __have_xlrd(), reason="missing xlrd")
def test_xlsx_borked(capsys):
    with pytest.raises(SystemExit) as exit_:
        pth = _get_path("sheet", extension="xlsx")
        cmd = "open excel {} {} {}".format(pth, "--skiprows=4", "--sheet_name=None")
        _call(cmd)

    errm = 'Specify --sheet_name="a sheet with spaces|the other sheet"'
    assert str(exit_.value) == errm
