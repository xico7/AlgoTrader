#####
## This file is exclusively to start the script from the archived version
## The only code to exist on this file is for normalizing the differences between the archived version and expanded version
####
import os
import sys
import tempfile
import time
import traceback
from pathlib import Path
from zipfile import ZipFile

if os.name == 'nt':
    venv = 'libs_win'
else:
    venv = 'libs_posix'

if os.environ.get('EXPERIMENT__MAIN_', False):  # For testing __main__.py in the IDE.
    container = Path("AlgoTrader.py")
else:
    container = Path(__file__).parent


# Partial dearchiving is necessary for packages with binaries
with tempfile.TemporaryDirectory(prefix="AlgoTrader") as dearchived_dir:
    dearchived_dir = Path(dearchived_dir)
    print("tmpdir:", dearchived_dir, file=sys.stderr)

    own_zipfile = ZipFile(container)

    must_extract = []

    for zipped_file in own_zipfile.filelist:
        filename = zipped_file.filename
        file = Path(filename)
        if file.parts[0] == venv and any(suffix in ('.pem', '.pyd', '.so', '.json') for suffix in file.suffixes):
            must_extract.append(file.parts[1])

    for zipped_file in own_zipfile.filelist:
        filename = zipped_file.filename
        file = Path(filename)

        try:
            if file.parts[0] == venv and file.parts[1] in must_extract:
                own_zipfile.extract(filename, dearchived_dir)
        except IndexError:
            continue

    sys.path.insert(0, str(Path(__file__).parent / venv))
    sys.path.insert(0, str(dearchived_dir / venv))


    # unlike the usual this import is the last thing to execute
    try:
        import main
    except ImportError:
        traceback.print_exc()
        print("tmpdir:", dearchived_dir, file=sys.stderr)
        print("waiting for interrupt or 40s", file=sys.stderr)
        time.sleep(40)
