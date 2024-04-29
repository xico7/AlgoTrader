from MongoDB.db_actions import DBMapper


def metrics_parser(args):
    metric_db_name = args['metric_db_mapper_name']

    metric_ti_class_to_instantiate = getattr(DBMapper, metric_db_name).value.metric_class
    instantiated_metric_class = metric_ti_class_to_instantiate(metric_db_name)
    instantiated_metric_class.parse_metric()
