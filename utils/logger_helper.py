import os
import datetime
from pytz import timezone
from logging import Filter
import logging.config


class InfoFilter(logging.Filter):
    def filter(self, rec):
        return rec.levelno == logging.INFO


class DebugFilter(logging.Filter):
    def filter(self, rec):
        return rec.levelno == logging.DEBUG


class WarnFilter(logging.Filter):
    def filter(self, rec):
        return rec.levelno == logging.WARN


class ErrorFilter(logging.Filter):
    def filter(self, rec):
        return rec.levelno == logging.ERROR


class CriticalFilter(logging.Filter):
    def filter(self, rec):
        return rec.levelno == logging.CRITICAL


COLORS = {
    'HEADER': '\033[95m',
    'INFO': '\033[92m',
    'DEBUG': '\033[94m',
    'WARNING': '\033[93m',
    'ERROR': '\033[95m',
    'CRITICAL': '\033[91m',
    'ENDC': '\033[0m',
}


class ColorFulFormatColMixin:
    def format_col(self, message_str, level_name):
        if level_name in COLORS.keys():
            message_str = COLORS.get(level_name) + message_str + COLORS.get(
                'ENDC')
        return message_str


class ColorfulFormatter(logging.Formatter, ColorFulFormatColMixin):
    def format(self, record):
        message_str = super(ColorfulFormatter, self).format(record)

        return self.format_col(message_str, level_name=record.levelname)


def config(log_level, log_path, name, tz='UTC'):
    def build_log_file(level, log_path, name, tz):
        utc_now = datetime.datetime.utcnow()
        utc_tz = timezone('UTC')
        local_tz = timezone(tz)
        tznow = utc_now.replace(tzinfo=utc_tz).astimezone(local_tz)
        return '{}-{}-{}.log'.format(os.path.join(log_path, name), tznow.strftime("%m-%d-%Y-%H:%M:%S"),
                                     level)

    if not os.path.exists(log_path):
        os.makedirs(log_path)

    LOGGING = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'default': {
                'format': '[%(asctime)s-%(levelname)s-%(name)s]: %(message)s (%(filename)s:%(lineno)s)'
            },
            'colorful_console': {
                'format': '[%(asctime)s-%(levelname)s-%(name)s]: %(message)s (%(filename)s:%(lineno)s)',
                '()': ColorfulFormatter,
            },
        },
        'filters': {
            'InfoFilter': {
                '()': InfoFilter,
            },
            'DebugFilter': {
                '()': DebugFilter,
            },
            'WarnFilter': {
                '()': WarnFilter,
            },
            'ErrorFilter': {
                '()': ErrorFilter,
            },
            'CriticalFilter': {
                '()': CriticalFilter,
            },
        },
        'handlers': {
            'milvus_celery_console': {
                'class': 'logging.StreamHandler',
                'formatter': 'colorful_console',
            },
            'milvus_debug_file': {
                'level': 'DEBUG',
                'filters': ['DebugFilter'],
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'default',
                'filename': build_log_file('debug', log_path, name, tz)
            },
            'milvus_info_file': {
                'level': 'INFO',
                'filters': ['InfoFilter'],
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'default',
                'filename': build_log_file('info', log_path, name, tz)
            },
            'milvus_warn_file': {
                'level': 'WARN',
                'filters': ['WarnFilter'],
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'default',
                'filename': build_log_file('warn', log_path, name, tz)
            },
            'milvus_error_file': {
                'level': 'ERROR',
                'filters': ['ErrorFilter'],
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'default',
                'filename': build_log_file('error', log_path, name, tz)
            },
            'milvus_critical_file': {
                'level': 'CRITICAL',
                'filters': ['CriticalFilter'],
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'default',
                'filename': build_log_file('critical', log_path, name, tz)
            },
        },
        'loggers': {
            '': {
                'handlers': ['milvus_celery_console', 'milvus_info_file', 'milvus_debug_file', 'milvus_warn_file',
                             'milvus_error_file', 'milvus_critical_file'],
                'level': log_level,
                'propagate': False
            },
        },
        'propagate': False,
    }

    logging.config.dictConfig(LOGGING)
