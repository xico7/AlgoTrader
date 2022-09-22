## Config variables, variables that can change how the program behaves are inserted here.

AGGTRADE_PYCACHE = 1000
ONE_HOUR_IN_MS = 60 * 60 * 1000
DEFAULT_PARSE_INTERVAL = 10
DEFAULT_TIMEFRAME_IN_MS = ONE_HOUR_IN_MS
DEFAULT_COL_SEARCH = 'BTCUSDT'

FUND_DB = "10_seconds_fund_data"
PARSED_TRADES_BASE_DB = "{}_seconds_parsed_trades"
PARSED_AGGTRADES_DB = "parsed_aggtrades"
AGGTRADES_DB = "aggtrades"


SP500_SYMBOLS_USDT_PAIRS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT', 'XRPUSDT', 'DOTUSDT',
                            'DOGEUSDT',
                            'AVAXUSDT', 'MATICUSDT', 'LTCUSDT', 'UNIUSDT', 'LINKUSDT',
                            'ALGOUSDT']


## Program constants, variables that contain 'static' values that are used in one or more modules.

coingecko_marketcap_api_link = "https://api.coingecko.com/api/v3/coins/" \
                               "markets?vs_currency=usd&order=market_cap_desc&per_page=150&page=1&sparkline=false"
SYMBOL = 'symbol'
DEFAULT_SYMBOL_SEARCH = "BTCUSDT"
MARKETCAP = 'marketcap'
TS = "timestamp"
PRICE = "price"
QUANTITY = 'quantity'


class MongoDB:
    EQUAL = '$eq'
    LOWER_EQ = '$lte'
    LOWER = '$lt'
    HIGHER_EQ = '$gte'
    HIGHER = '$gt'
    AND = '$and'


END_TS = "end_timestamp"
#
# #### Timeframes ####
#
SECONDS_TO_MS_APPEND = '000'
#
# WEEK_DAYS = 7
TEN_SECONDS = 10
ONE_MIN_IN_SECS = 60
# FIVE_MIN_IN_SECS = ONE_MIN_IN_SECS * 5
# FIFTEEN_MIN_IN_SECS = ONE_MIN_IN_SECS * 15
# THIRTY_MIN_IN_SECS = FIFTEEN_MIN_IN_SECS * 2
# ONE_HOUR_IN_SECS = ONE_MIN_IN_SECS * 60
# FOUR_HOUR_IN_SECS = ONE_HOUR_IN_SECS * 4
# ONE_DAY_IN_SECS = ONE_HOUR_IN_SECS * 24
#
# ONE_DAY_IN_MS = ONE_DAY_IN_SECS * 1000
# ONE_HOUR_IN_MS = ONE_HOUR_IN_SECS * 1000
# FIFTEEN_MIN_IN_MS = int(str(FIFTEEN_MIN_IN_SECS) + SECONDS_TO_MS_APPEND)
ONE_MIN_IN_MS = int(str(ONE_MIN_IN_SECS) + SECONDS_TO_MS_APPEND)
TEN_SECS_MS = int(str(TEN_SECONDS) + SECONDS_TO_MS_APPEND)
FIVE_SECS_IN_MS = 5000
#
# ###################
#
USDT = "USDT"
BNB = "BNB"
