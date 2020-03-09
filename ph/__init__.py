#!/usr/bin/env python
from __future__ import print_function
import pandas as pd

COMMANDS = {}

def help(*args, **kwargs):
    print('Usage: ph command arguments')
    print('       commands = {}'.format(list(COMMANDS.keys())))


def open(*args, **kwargs):
    if not len(args):
        exit('Missing argument to open')
    df = pd.read_csv(args[0])
    print(df.to_csv(sep=',', index=False))



COMMANDS['help'] = help
COMMANDS['open'] = open


def main():
    import sys
    args = sys.argv
    if len(args) < 2:
        exit('Usage: ph command [args]\n       ph help')
    cmd = args[1]
    if cmd not in COMMANDS:
        exit('Unknown command {cmd}.')
    COMMANDS[cmd](*args[2:])

if __name__ == '__main__':
    main()
