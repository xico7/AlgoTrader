#! /usr/bin/env python
import os
import zipapp
import sys
from functools import partial
from pathlib import Path

printe = partial(print, file=sys.stderr)

name = 'AlgoTrader'

libs_posix = 'libs_posix'

if os.name == 'nt':
    raise NotImplementedError("Windows package is not implemented.")

os.system(f'"{sys.executable}" -m pip install -t libs_posix --upgrade -r requirements.txt')


def into_package_filter(f: Path):
    return (
            f.name not in (
        name + '.py',
        name + '.zip',
    ) and (
                    'libs_posix' in f.parts or
                    f.suffix not in (
                        '.pem',
                        '.pub',
                        '.log',
                    )
            ) and
            'venv' not in f.parts and
            'venv_win' not in f.parts and
            '__pycache__' not in f.parts and
            '.git' not in f.parts and
            '.idea' not in f.parts
    )


printe("packaging...")

zipapp.create_archive('', name + '.py', "/usr/bin/env python3", filter=into_package_filter)

printe("done")
