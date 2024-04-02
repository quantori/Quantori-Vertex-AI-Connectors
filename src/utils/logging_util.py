import logging
import json
import os
import sys

LEVEL = os.environ.get("LOG_LEVEL", "DEBUG")
LOG_FORMAT = os.environ.get("LOG_FORMAT", "CLOUD")


class CloudLoggingHandler(logging.StreamHandler):
    def __init__(self, logger_name):
        super().__init__()
        self.logger_name = logger_name

    def emit(self, record):
        log_entry = self.format(record)
        stream = sys.stdout if record.levelno < logging.WARNING else sys.stderr
        stream.write(log_entry + '\n')
        stream.flush()

    def format(self, record):
        log_entry = {
            'severity': record.levelname,
            'message': super().format(record),
            'labels': {
                'logger_name': self.logger_name,
            },
        }
        if hasattr(record, 'pathname') and hasattr(record, 'lineno'):
            log_entry['logging.googleapis.com/sourceLocation'] = {
                'file': record.pathname,
                'line': record.lineno,
                'function': record.funcName,
            }
        return json.dumps(log_entry)


def get_logger(name):
    if LOG_FORMAT == "CLOUD":
        handler = CloudLoggingHandler(logger_name=name)
        handler.setFormatter(logging.Formatter('%(message)s'))
    elif LOG_FORMAT == "LOCAL":
        handler = logging.StreamHandler(sys.stdout)
    else:
        raise ValueError(f"Invalid LOG_FORMAT: {LOG_FORMAT}")
    logger = logging.getLogger(name)
    logger.addHandler(handler)
    logger.setLevel(LEVEL)
    return logger