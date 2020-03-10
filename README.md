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

Suppose you `a.csv` that looks like this:

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
cat a.csv | ph transpose
```

`abs` (as well as many others, e.g.  `corr`, `count`, `cov`, `cummax`, `cumsum`,
`diff`, `max`, `median`, `product`, `quantile`, `rank`, `round`, `sum`, `std`,
`var` etc.):

```bash
cat a.csv | ph abs
```

**Use `ph help` to list all commands**


Using `head` and `tail` works approximately as the normal shell equivalents,
however they will preserve the header if there is one, e.g.

```bash
cat a.csv | ph head 20 | ph tail
```

If the `csv` file contains a column, e.g. named `t` containing timestamps, it
can be parsed as such with `ph date t`:

```bash
cat a.csv | ph date t
```

The normal Pandas `describe` is of course available:

```bash
cat a.csv | ph describe
```

Selecting only certain columns, e.g. `a` and `b`

```bash
cat a.csv | ph columns a b
```

You can sum two columns `x` and `y` and place the result in column `z` using
`apply`

```bash
cat a.csv | ph apply + x y z
```

If you only want the sum of two columns, then, you can pipe the last two using

```bash
cat a.csv| ph apply + x y z | ph columns z
```

You can normalize a column using `ph normalize col`.

```bash
cat a.csv | ph apply \* x y z | ph normalize z
x,y,z
3,8,0.0
4,9,0.15
5,10,0.325
6,11,0.525
7,12,0.75
8,13,1.0
```
