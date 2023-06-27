from MongoDB.db_actions import DBMapper
import importlib


def metrics_parser(args):
    metric_db_name = args['metric_db_mapper_name']
    metric_data = getattr(DBMapper, metric_db_name).value

    ta_indicators_module = importlib.import_module("tasks.technical_indicators.technical_indicators")
    metric_parser = getattr(ta_indicators_module, metric_data.metric_class)(metric_data, metric_db_name)
    metric_parser.parse_metric(timeframe_based=metric_data.timeframe_based)

