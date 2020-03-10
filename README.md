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

You can sum two columns `x` and `y` and place the result in column `z` using
`apply`

```bash
$ cat a.csv | ph apply + x y z
x,y,z
3,8,11
4,9,13
5,10,15
6,11,17
7,12,19
8,13,21

```

If you only want the sum of two columns, then, you can pipe the last two using

```bash
$ cat a.csv| ph apply + x y z | ph columns z
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
$ cat a.csv | ph apply \* x y z | ph normalize z
x,y,z
3,8,0.0
4,9,0.15
5,10,0.325
6,11,0.525
7,12,0.75
8,13,1.0
```
