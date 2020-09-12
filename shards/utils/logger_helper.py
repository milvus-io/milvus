import os
import datetime
import copy
from pytz import timezone
from logging import Filter
import logging.config
from utils import colors


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
    'HEADER': colors.BWhite,
    'INFO': colors.On_IWhite + colors.BBlack,
    'INFOM': colors.White,
    'DEBUG': colors.On_IBlue + colors.BWhite,
    'DEBUGM': colors.BIBlue,
    'WARNING': colors.On_IYellow + colors.BWhite,
    'WARNINGM': colors.BIYellow,
    'ERROR': colors.On_IRed + colors.BWhite,
    'ERRORM': colors.BIRed,
    'CRITICAL': colors.On_Red + colors.BWhite,
    'CRITICALM': colors.BRed,
    'ASCTIME': colors.On_Cyan + colors.BIYellow,
    'MESSAGE': colors.IGreen,
    'FILENAME': colors.BCyan,
    'LINENO': colors.BCyan,
    'THREAD': colors.BCyan,
    'ENDC': colors.Color_Off,
}


class ColorFulFormatColMixin:
    def format_col(self, message_str, level_name):
        if level_name in COLORS.keys():
            message_str = COLORS[level_name] + message_str + COLORS['ENDC']
        return message_str

    def formatTime(self, record, datefmt=None):
        ret = super().formatTime(record, datefmt)
        ret = COLORS['ASCTIME'] + ret + COLORS['ENDC']
        return ret


class ColorfulLogRecordProxy(logging.LogRecord):
    def __init__(self, record):
        self._record = record
        msg_level = record.levelname + 'M'
        self.msg = '{}{}{}'.format(COLORS[msg_level], record.msg, COLORS['ENDC'])
        self.filename = COLORS['FILENAME'] + record.filename + COLORS['ENDC']
        self.lineno = '{}{}{}'.format(COLORS['LINENO'], record.lineno, COLORS['ENDC'])
        self.threadName = '{}{}{}'.format(COLORS['THREAD'], record.threadName, COLORS['ENDC'])
        self.levelname = COLORS[record.levelname] + record.levelname + COLORS['ENDC']

    def __getattr__(self, attr):
        if attr not in self.__dict__:
            return getattr(self._record, attr)
        return getattr(self, attr)


class ColorfulFormatter(ColorFulFormatColMixin, logging.Formatter):
    def format(self, record):
        proxy = ColorfulLogRecordProxy(record)
        message_str = super().format(proxy)

        return message_str


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
                'format': '%(asctime)s | %(levelname)s | %(name)s | %(threadName)s: %(message)s (%(filename)s:%(lineno)s)',
            },
            'colorful_console': {
                'format': '%(asctime)s | %(levelname)s: %(message)s (%(filename)s:%(lineno)s) (%(threadName)s)',
                # 'format': '%(asctime)s | %(levelname)s | %(threadName)s: %(message)s (%(filename)s:%(lineno)s)',
                # 'format': '%(asctime)s | %(levelname)s | %(name)s | %(threadName)s: %(message)s (%(filename)s:%(lineno)s)',
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
