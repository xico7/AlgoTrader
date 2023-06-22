import logging

import logs
from MongoDB.db_actions import DBCol
from data_handling.data_helpers.vars_constants import BASE_TRADES_CHART_DB
from support.generic_helpers import get_key_values_from_dict_with_dicts


LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


def get_trades_chart_metric_distribution(args):
    LOG.info(f"Starting to parse trades chart metric {args['trades_chart_metric_name']} for symbol {args['symbol']}.")
    distribution_values = {}
    counter = 0
    for trade_chart_value in DBCol(BASE_TRADES_CHART_DB.format(args['trades_chart_timeframe']), args['symbol']).find_all():
        metric_value = get_key_values_from_dict_with_dicts(trade_chart_value, args['trades_chart_metric_name'])
        metric_value = str(round(metric_value) if isinstance(metric_value, float) else 0)

        try:
            distribution_values[metric_value] += 1
        except KeyError:
            distribution_values[metric_value] = 1
        counter +=1

        if counter > 10000:
            break

    DBCol("Alpha_algo_stuff", "trades_chart_distribution").insert_many([{'symbol': args['symbol'], 'distribution_values': distribution_values}])

    LOG.info("DONE")
    exit(0)

