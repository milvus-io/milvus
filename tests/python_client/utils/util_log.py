import logging
import sys

from config.log_config import log_config


class TestLog:
    def __init__(self, logger):
        self.logger = logger
        self.log = logging.getLogger(self.logger)
        self.log.setLevel(logging.DEBUG)

        # Only add console handler if needed (commented out by default)
        # All file logging is handled by ConditionalLogHandler plugin
        try:
            formatter = logging.Formatter("[%(asctime)s - %(levelname)s - %(name)s]: "
                                          "%(message)s (%(filename)s:%(lineno)s)")

            # Stream handler (commented out by default)
            ch = logging.StreamHandler(sys.stdout)
            ch.setLevel(logging.DEBUG)
            ch.setFormatter(formatter)
            # self.log.addHandler(ch)

        except Exception as e:
            print("Failed to initialize logger: %s" % str(e))


"""All modules share this unified log"""
test_log = TestLog('ci_test').log
