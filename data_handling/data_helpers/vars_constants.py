from enum import Enum

AGGTRADE_PYCACHE = 1000
ONE_HOUR_IN_MS = 60 * 60 * 1000
ONE_DAY_IN_MS = ONE_HOUR_IN_MS * 24
DEFAULT_PARSE_INTERVAL_SECONDS = 10
DEFAULT_PARSE_INTERVAL_IN_MS = DEFAULT_PARSE_INTERVAL_SECONDS * 1000
DEFAULT_TIMEFRAME_IN_MS = ONE_HOUR_IN_MS
DEFAULT_COL_SEARCH = 'BTCUSDT'
FUND_DATA_COLLECTION = "fund_data"
TEN_SECS_PARSED_TRADES_DB = "ten_seconds_parsed_trades"
PARSED_AGGTRADES_DB = "parsed_aggtrades"
VALIDATOR_DB = "Timestamps_Validator"
START_TS_VALIDATOR_DB_SUFFIX = "_start_timestamp_validator"
FINISH_TS_VALIDATOR_DB_SUFFIX = "_finish_timestamp_validator"
DONE_INTERVAL_VALIDATOR_DB_SUFFIX = "_done_interval_timestamp_validator"
VALID_END_TS_VALIDATOR_DB_SUFFIX = "_valid_end_timestamp_validator"
END_TS_AGGTRADES_VALIDATOR_DB = "end_timestamp_aggtrades_validator_db"
START_TS_AGGTRADES_VALIDATOR_DB = "start_timestamp_aggtrades_validator_db"
TRADES_CHART_DB = 'trades_chart_db'
TRADES_CHART_DB_ALL = 'trades_chart_db_all'
REL_VOLUME_DB_CONTAINING_NAME = 'relative_volume'
BASE_TRADES_CHART_DB = TRADES_CHART_DB + '_{}_minutes'
TRADE_DATA_CACHE_TIME_IN_MINUTES = 1440


DEEMED_UNTRADEABLE_SYMBOLS = ['USDTIDRT', 'USDTTRY', 'BUSDUSDT', 'USDTRUB', 'USDTBRL', 'USTUSDT',
                              'BNBUSDT', 'TUSDUSDT', 'EURUSDT']  # Symbols that i don't consider tradeable for various reasons.
NO_LONGER_TRADED_BINANCE_SYMBOLS = ['USDTNGN', 'USDPUSDT', 'WNXMUSDT', 'LOKAUSDT', 'POWRUSDT', 'API3USDT', 'LOKAUSDT',
                              'IMXUSDT', 'ANCUSDT', 'WOOUSDT', 'BURGERUSDT', 'WANUSDT', 'USDTBIDR', 'ICPUSDT']  # Some symbols stop being traded in Binance.

UNUSED_CHART_TRADE_SYMBOLS = NO_LONGER_TRADED_BINANCE_SYMBOLS + DEEMED_UNTRADEABLE_SYMBOLS
FUND_SYMBOLS_USDT_PAIRS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT', 'XRPUSDT', 'DOTUSDT', 'DOGEUSDT',
                           'AVAXUSDT', 'MATICUSDT', 'LTCUSDT', 'UNIUSDT', 'LINKUSDT', 'ALGOUSDT']

# This index is used to determine which is the main document key for the collection timeframe
TIMEFRAME_DOC_KEY_INDEX = 'timeframe_index_placeholder_name'

## Program constants, variables that contain 'static' values that are used in one or more modules.

coingecko_marketcap_api_link = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=150&page=1&sparkline=false"
SYMBOL = 'symbol'
MARKETCAP = 'marketcap'
TS = "timestamp"
FINISH_TS = "finish_timestamp"
VALID_END_TS = "valid_end_timestamp"
START_TS = 'start_timestamp'
PRICE = "price"
QUANTITY = 'quantity'


class DBQueryOperators(Enum):
    EQUAL = '$eq'
    LOWER_EQ = '$lte'
    LOWER = '$lt'
    HIGHER_EQ = '$gte'
    HIGHER = '$gt'
    AND = '$and'
    IN = '$in'


#

# #### Timeframes ####
#

TWO_HOURS_IN_MINUTES = 120
ONE_DAY_IN_MINUTES = 1440
ONE_DAY_IN_MS = ONE_DAY_IN_MINUTES * 60 * 1000


SECONDS_TO_MS_APPEND = '000'
TEN_SECONDS = 10
ONE_MIN_IN_SECS = 60
ONE_MIN_IN_MS = int(str(ONE_MIN_IN_SECS) + SECONDS_TO_MS_APPEND)
TEN_MIN_IN_MS = ONE_MIN_IN_MS * 10
THIRTY_MINS_IN_MS = TEN_MIN_IN_MS * 3
TEN_SECONDS_IN_MS = 10000
FIVE_SECS_IN_MS = 5000
ONE_SECONDS_IN_MS = 1000
#
# ###################
#
USDT = "USDT"
BNB = "BNB"

