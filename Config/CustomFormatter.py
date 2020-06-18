import logging


class CustomFormatter(logging.Formatter):
    """Logging Formatter to add colors"""

    BOLD = '\033[1m'
    DEBUG = '\033[92m'
    INFO = '\033[94m'
    WARNING = '\033[93m'
    ERROR = '\033[91m'
    END = '\033[0m'
    prefix = "[%(asctime)s] "
    suffix = ": %(message)s"

    source = " %(lineno)4d@%(module)-15s@%(name)s "
    suffix = source + suffix

    COLOR_FORMATS = {
        logging.DEBUG: prefix + BOLD + DEBUG + "CC_DEBUG" + END + END + suffix,
        logging.INFO: prefix + BOLD + INFO + "CC_INFO" + END + END + suffix,
        logging.WARNING: prefix + BOLD + WARNING + "CC_WARNING" + END + END + suffix,
        logging.ERROR: prefix + BOLD + ERROR + "CC_ERROR" + END + END + suffix,
    }

    REGULAR_FORMATS = {
        logging.DEBUG: prefix + "CC_DEBUG" + suffix,
        logging.INFO: prefix + "CC_INFO" + suffix,
        logging.WARNING: prefix + "CC_WARNING" + suffix,
        logging.ERROR: prefix + "CC_ERROR" + suffix,
    }

    def __init__(self, use_colors=True):
        super(CustomFormatter, self).__init__()
        if use_colors:
            self.formats = self.COLOR_FORMATS
        else:
            self.formats = self.REGULAR_FORMATS

    def format(self, record):
        log_fmt = self.formats.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)
