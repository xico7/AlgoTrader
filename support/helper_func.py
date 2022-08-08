import logging
import traceback

import logs

LOG = logging.getLogger(logs.LOG_BASE_NAME + '.' + __name__)


def output_error(message, error_code=1):
    LOG.exception(message)
    traceback.print_exc()
    exit(error_code)
