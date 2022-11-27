## Config variables, variables that can change how the program behaves are inserted here.
from binance import Client
from data_handling.data_helpers.secrets import BINANCE_API_KEY, BINANCE_API_SECRET

binance_client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)


AGGTRADE_PYCACHE = 1000
ONE_HOUR_IN_MS = 60 * 60 * 1000
ONE_DAY_IN_MS = ONE_HOUR_IN_MS * 24
DEFAULT_PARSE_INTERVAL = 10
DEFAULT_PARSE_INTERVAL_IN_MS = DEFAULT_PARSE_INTERVAL * 1000
DEFAULT_TIMEFRAME_IN_MS = ONE_HOUR_IN_MS
DEFAULT_COL_SEARCH = 'BTCUSDT'
FUND_COL = "fund_data"
TEN_SECS_PARSED_TRADES_DB = "ten_seconds_parsed_trades"
PARSED_AGGTRADES_DB = "parsed_aggtrades"
VALIDATOR_DB = "Timestamps_Validator"
START_TS_VALIDATOR_DB_SUFFIX = "_start_timestamp_validator"
END_TS_VALIDATOR_DB_SUFFIX = "_end_timestamp_validator"
END_TS_AGGTRADES_VALIDATOR_DB = "end_timestamp_aggtrades_validator_db"
START_TS_AGGTRADES_VALIDATOR_DB = "start_timestamp_aggtrades_validator_db"


TRADE_DATA_PYTHON_CACHE_SIZE = 30
TRADE_DATA_CACHE_TIME_IN_MS = 60 * 60 * 1000


DEEMED_UNTRADEABLE_SYMBOLS = ['USDTIDRT', 'USDTTRY', 'BUSDUSDT', 'USDTRUB', 'USDTBRL', 'USTUSDT',
                              'BNBUSDT', 'TUSDUSDT']  # Symbols that i don't consider tradeable for various reasons.
NO_LONGER_TRADED_BINANCE_SYMBOLS = ['USDTNGN', 'USDPUSDT', 'WNXMUSDT', 'LOKAUSDT', 'POWRUSDT', 'API3USDT', 'LOKAUSDT',
                              'IMXUSDT', 'ANCUSDT', 'WOOUSDT', 'BURGERUSDT']  # Some symbols stop being traded in Binance.

UNUSED_CHART_TRADE_SYMBOLS = NO_LONGER_TRADED_BINANCE_SYMBOLS + DEEMED_UNTRADEABLE_SYMBOLS
FUND_SYMBOLS_USDT_PAIRS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT', 'XRPUSDT', 'DOTUSDT', 'DOGEUSDT',
                           'AVAXUSDT', 'MATICUSDT', 'LTCUSDT', 'UNIUSDT', 'LINKUSDT', 'ALGOUSDT', 'IMXUSDT', 'GLMRUSDT']


## Program constants, variables that contain 'static' values that are used in one or more modules.

coingecko_marketcap_api_link = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=150&page=1&sparkline=false"
SYMBOL = 'symbol'
DEFAULT_SYMBOL_SEARCH = "BTCUSDT"
MARKETCAP = 'marketcap'
TS = "timestamp"
END_TS = "end_timestamp"
START_TS = 'start_timestamp'
PRICE = "price"
QUANTITY = 'quantity'


class MongoDB:
    EQUAL = '$eq'
    LOWER_EQ = '$lte'
    LOWER = '$lt'
    HIGHER_EQ = '$gte'
    HIGHER = '$gt'
    AND = '$and'


#
# #### Timeframes ####
#
SECONDS_TO_MS_APPEND = '000'
TEN_SECONDS = 10
ONE_MIN_IN_SECS = 60
ONE_MIN_IN_MS = int(str(ONE_MIN_IN_SECS) + SECONDS_TO_MS_APPEND)
TEN_MIN_IN_MS = ONE_MIN_IN_MS * 10
THIRTY_MINS_IN_MS = TEN_MIN_IN_MS * 3
TEN_SECS_MS = 10000
FIVE_SECS_IN_MS = 5000
ONE_SECONDS_IN_MS = 1000
#
# ###################
#
USDT = "USDT"
BNB = "BNB"
