#!/usr/bin/env python
from __future__ import print_function
import pandas as pd
import sys

COMMANDS = {}


def register(fn):
    COMMANDS[fn.__name__] = fn
    return fn


def pipeout(df):
    print(df.to_csv(sep=",", index=False))


def pipein():
    return pd.read_csv(sys.stdin)


@register
def help(*args, **kwargs):
    print("Usage: ph command arguments")
    print("       commands = {}".format(list(COMMANDS.keys())))


@register
def open(fname):
    df = pd.read_csv(fname)
    pipeout(df)


@register
def head(n):
    pipeout(pipein().head(int(n)))


@register
def tail(n):
    pipeout(pipein().tail(int(n)))


@register
def columns(*cols):
    pipeout(pipein()[list(cols)])


@register
def shape():
    print("rows,columns\n" + ",".join([str(x) for x in pipein().shape]))


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
