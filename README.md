# ph - the tabular data shell tool

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
