import argparse

PROGRAM_NAME = 'Algotrading-Crypto'

def algo_argparse():
    parser = argparse.ArgumentParser(
        prog=PROGRAM_NAME,
        fromfile_prefix_chars='@'
    )

    parser.add_argument("-v", "--verbosity", default=0, action="count",
                               help="increase logging verbosity (up to 5)")
    parser.add_argument('--run-ws-trades-mongodb', action="store_true",
                        help="asdf.")
    parser.add_argument('--run-ta-analysis', default=0, required=False, type=int,
                        help="Run ta analysis with starting date 'current hour'- Argument value.")
    parser.add_argument('--run-ta-signal', default=0, required=False, type=int,
                        help="Run ta signal with starting date 'current hour'- Argument value.")
    parser.add_argument('--run-alpha-algo', action="store_true",
                        help="Run ta signal with starting date 'current hour'- Argument value.")

    return parser.parse_args()
