# ph - the tabular data shell tool

Spoiler: Working with tabular data in the command line is difficult.  `ph` makes
it easy:

```bash
$ pip install ph
$ cat iris.csv | ph columns 4 150 | ph head 15 | ph tail 5 | ph tabulate --headers
      4    150
--  ---  -----
 0  3.7    5.4
 1  3.4    4.8
 2  3      4.8
 3  3      4.3
 4  4      5.8
```

```bash
$ cat iris.csv | ph describe
              150           4      setosa  versicolor   virginica
count  150.000000  150.000000  150.000000  150.000000  150.000000
mean     5.843333    3.057333    3.758000    1.199333    1.000000
std      0.828066    0.435866    1.765298    0.762238    0.819232
min      4.300000    2.000000    1.000000    0.100000    0.000000
25%      5.100000    2.800000    1.600000    0.300000    0.000000
50%      5.800000    3.000000    4.350000    1.300000    1.000000
75%      6.400000    3.300000    5.100000    1.800000    2.000000
max      7.900000    4.400000    6.900000    2.500000    2.000000
```

Occasionally you would like to plot a CSV file real quick, in which case you can
simply pipe it to `ph plot`:

Suppose you have a dataset `covid.csv`

```csv
SK,Italy,Iran,France,Spain,US
51,79,95,57,84,85
104,150,139,100,125,111
204,227,245,130,169,176
433,320,388,191,228,252
602,445,593,212,282,352
833,650,978,285,365,495
977,888,1501,423,430,640
1261,1128,2336,613,674,926
1766,1694,2922,949,1231,NaN
2337,2036,3513,1126,1696,NaN
3150,2502,4747,1412,NaN,NaN
4212,3089,5823,1748,NaN,NaN
4812,3858,6566,NaN,NaN,NaN
5328,4638,7161,NaN,NaN,NaN
5766,5883,8042,NaN,NaN,NaN
6284,7375,NaN,NaN,NaN,NaN
6767,9172,NaN,NaN,NaN,NaN
7134,10149,NaN,NaN,NaN,NaN
7382,NaN,NaN,NaN,NaN,NaN
7513,NaN,NaN,NaN,NaN,NaN
```

With this simple command, you get a certified _"So fancy" plot_.

```bash
$ cat covid.csv | ph plot
```

![So fancy covid plot](https://raw.githubusercontent.com/pgdr/ph/master/assets/covid-plot.png)



---

## Raison d'être

Using the _pipeline_ in Linux is nothing short of a dream in the life of the
computer super user.

However the pipe is clearly most suited for a stream of lines of textual data,
and not when the stream is actually tabular data.

Tabular data is much more complex to work with due to its dual indexing and the
fact that we often read horizontally and often read vertically.

The defacto format for tabular data is `csv` (which is not perfect in any sense
of the word), and the defacto tool for working with tabular data in Python is
Pandas.

This is a shell utility `ph` that reads tabular data from standard in and allows
you to perform a pandas function on the data, before writing it to standard out
in `csv` format.

The goal is to create a tool which makes it nicer to work with tabular data in a
pipeline.

## Getting started

If you have installed `ph[data]`, you can try out the examples above using

```bash
ph dataset boston | ph describe
```

Available datasets are from
[scikit-learn.datasets](https://scikit-learn.org/stable/datasets/index.html)

Toy datasets:

* `boston`
* `iris`
* `diabetes`
* `digits`
* `linnerud`
* `wine`
* `breast_cancer`


Real world:

* `olivetti_faces`
* `20newsgroups`
* `20newsgroups_vectorized`
* `lfw_people`
* `lfw_pairs`
* `covtype`
* `rcv1`
* `kddcup99`
* `california_housing`


## Example usage

Suppose you have a csv file `a.csv` that looks like this:

```csv
x,y
3,8
4,9
5,10
6,11
7,12
8,13
```

Transpose:

```bash
$ cat a.csv | ph transpose
0,1,2,3,4,5
3,4,5,6,7,8
8,9,10,11,12,13
```

`median` (as well as many others, e.g.  `abs`, `corr`, `count`, `cov`, `cummax`,
`cumsum`, `diff`, `max`, `product`, `quantile`, `rank`, `round`, `sum`, `std`,
`var` etc.):

```bash
$ cat a.csv | ph median
0
5.5
10.5
```

**Use `ph help` to list all commands**


Using `head` and `tail` works approximately as the normal shell equivalents,
however they will preserve the header if there is one, e.g.

```bash
$ cat a.csv | ph head 7 | ph tail 3
x,y
6,11
7,12
8,13
```

If the `csv` file contains a column, e.g. named `x` containing timestamps, it
can be parsed as such with `ph date x`:

```bash
$ cat a.csv | ph date x
x,y
1970-01-01 00:00:00.000000003,8
1970-01-01 00:00:00.000000004,9
1970-01-01 00:00:00.000000005,10
1970-01-01 00:00:00.000000006,11
1970-01-01 00:00:00.000000007,12
1970-01-01 00:00:00.000000008,13
```

To get a column with integers (e.g. 3-8) parsed as, e.g. 2003 - 2008, some
amount of hacking is necessary.  We will go into details later on the `eval` and
`appendstr`.

```bash
$ cat a.csv | ph eval "x = 2000 + x" | ph appendstr x "-01-01" | ph date x
x,y
2003-01-01,8
2004-01-01,9
2005-01-01,10
2006-01-01,11
2007-01-01,12
2008-01-01,13
```


The normal Pandas `describe` is of course available:

```bash
$ cat a.csv | ph describe
              x          y
count  6.000000   6.000000
mean   5.500000  10.500000
std    1.870829   1.870829
min    3.000000   8.000000
25%    4.250000   9.250000
50%    5.500000  10.500000
75%    6.750000  11.750000
max    8.000000  13.000000

```

Print the column names:

```bash
$ cat a.csv | ph columns
x
y
```

Selecting only certain columns, e.g. `a` and `b`

```bash
$ cat a.csv | ph columns x y
x,y
3,8
4,9
5,10
6,11
7,12
8,13
```

Rename:

```bash
$ cat a.csv | ph rename x a | ph rename y b
a,b
3,8
4,9
5,10
6,11
7,12
8,13
```


You can sum two columns `x` and `y` and place the result in column `z` using
`eval` (from
[`pandas.DataFrame.eval`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.eval.html#pandas.DataFrame.eval)).

```bash
$ cat a.csv | ph eval "z = x + y"
x,y,z
3,8,11
4,9,13
5,10,15
6,11,17
7,12,19
8,13,21
```


```bash
$ cat a.csv | ph eval "z = x**2 + y"
x,y,z
3,8,17
4,9,25
5,10,35
6,11,47
7,12,61
8,13,77
```


If you only want the sum of two columns, you can chain the two previous
commands:

```bash
$ cat a.csv | ph eval "z = x + y" | ph columns z
z
11
13
15
17
19
21
```

You can normalize a column using `ph normalize col`.

```bash
$ cat a.csv | ph eval "z = x * y" | ph normalize z
x,y,z
3,8,0.0
4,9,0.15
5,10,0.325
6,11,0.525
7,12,0.75
8,13,1.0
```

We can [query](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html) data using `ph query expr`.

```bash
$ cat a.csv | ph query "x > 5"
x,y
6,11
7,12
8,13
```

**Warning**:
Calling the following command
[might be illegal in Norway](https://rettspraksis.no/wiki/TBERG-2019-141281).
Reader beware!


```bash
$ ph open csv 'http://bit.ly/2cLzoxH' | ph query "country == 'Norway'" | ph tabulate --headers
    country      year          pop  continent      lifeExp    gdpPercap
--  ---------  ------  -----------  -----------  ---------  -----------
 0  Norway       1952  3.32773e+06  Europe          72.67       10095.4
 1  Norway       1957  3.49194e+06  Europe          73.44       11654
 2  Norway       1962  3.63892e+06  Europe          73.47       13450.4
 3  Norway       1967  3.78602e+06  Europe          74.08       16361.9
 4  Norway       1972  3.933e+06    Europe          74.34       18965.1
 5  Norway       1977  4.04320e+06  Europe          75.37       23311.3
 6  Norway       1982  4.11479e+06  Europe          75.97       26298.6
 7  Norway       1987  4.18615e+06  Europe          75.89       31541
 8  Norway       1992  4.28636e+06  Europe          77.32       33965.7
 9  Norway       1997  4.40567e+06  Europe          78.32       41283.2
10  Norway       2002  4.53559e+06  Europe          79.05       44684
11  Norway       2007  4.62793e+06  Europe          80.196      49357.2
```

## Tabulate

The amazing _tabulate_ tool comes from the Python package
[tabulate on PyPI](https://pypi.org/project/tabulate/).

The `tabulate` command takes arguments `--headers` to toggle printing of header
row, `--format=[grid,...]` to modify the table style and `--noindex` to remove
the running index (leftmost column in the example above).

Among the supported format styles are

* `plain`, `simple`,
* `grid`, `fancy_grid`, `pretty`,
* `github`, `rst`, `mediawiki`, `html`, `latex`,
* ... (See full list at the project homepage at
  [python-tabulate](https://github.com/astanin/python-tabulate).)


## Plotting data

You can plot data using `ph plot [--index=col]`.

```bash
$ ph open parquet 1A_2019.parquet | ph columns Time Value | ph plot --index=Time
```

This will take the columns `Time` and `Value` from the timeseries provided by
the given `parquet` file and plot the `Value` series using `Time` as _index_.


The following example plots the life expectancy in Norway using `year` as _index_:

```bash
$ ph open csv http://bit.ly/2cLzoxH  | ph query "country == 'Norway'" | ph appendstr year -01-01 | ph columns year lifeExp | ph plot --index=year
```

![life-expectancy over time](https://raw.githubusercontent.com/pgdr/ph/master/assets/lifeexp.png)

> _Note:_ The strange `ph appendstr year -01-01` turns the items `1956` into
> `"1956-01-01"` and `2005` into `"2005-01-01"`.  These are necessary to make
> pandas to interpret `1956` as a _year_ and not as a _millisecond_.
>
> The command `ph appendstr col str [newcol]` takes a string and appends it to a
> column, overwriting the original column, or writing it to `newcol` if provided.

**Advanced plotting**

You can choose the _kind_ of plotting ( ‘line’, ‘bar’, ‘barh’, ‘hist’, ‘box’,
‘kde’, ‘density’, ‘area’, ‘pie’, ‘scatter’, ‘hexbin’), the _style_ of plotting
(e.g. `--style=o`), and in case of scatter plot, you need to specify `--x=col1`
and `--y=col2`, e.g.:

```bash
$ ph open csv http://bit.ly/2cLzoxH | ph query "continent == 'Europe'" | ph plot --kind=scatter --x=lifeExp --y=gdpPercap
```

![life-expectancy vs gdp](https://raw.githubusercontent.com/pgdr/ph/master/assets/scatter.png)





To specify the styling `k--` gives a black dashed line:

```bash
$ ph open csv http://bit.ly/2cLzoxH  | ph query "country == 'Norway'" | ph appendstr year -01-01 | ph columns year lifeExp | ph plot --index=year --style=k--
```




## Working with different file types

Pandas supports reading a multitude of [readers](https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html).

To read an Excel file and pipe the stream, you can use `ph open`.

The syntax of `ph open` is `ph open ftype fname`, where `fname` is the
file you want to stream and `ftype` is the type of the file.

A list of all available formats is given below.

```bash
$ ph open xls a.xlsx
x,y
3,8
4,9
5,10
6,11
7,12
8,13
```

* `csv` / `tsv` (the latter for tab-separated values)
* `fwf` (fixed-width file format)
* `json`
* `html`
* `clipboard` (pastes tab-separated content from clipboard)
* `xls`
* `odf`
* `hdf5`
* `feather`
* `parquet`
* `orc`
* `stata`
* `sas`
* `spss`
* `pickle`
* `sql`
* `gbq` / `google` / `bigquery`
