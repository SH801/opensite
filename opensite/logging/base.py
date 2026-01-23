import logging
import sys

class ColorFormatter(logging.Formatter):
    """Custom Formatter to add colors to log levels for Terminal only."""
    
    BLUE = "\x1b[34m"    # Info
    ORANGE = "\x1b[33m"  # Warning
    RED = "\x1b[31m"     # Error
    WHITE = "\x1b[37m"   # Debug
    RESET = "\x1b[0m"

    FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

    LEVEL_COLORS = {
        logging.DEBUG: WHITE,
        logging.INFO: BLUE,
        logging.WARNING: ORANGE,
        logging.ERROR: RED,
    }

    def format(self, record):
        color = self.LEVEL_COLORS.get(record.levelno, self.RESET)
        log_fmt = f"{color}{self.FORMAT}{self.RESET}"
        # We create a temporary formatter with the colorized string
        formatter = logging.Formatter(log_fmt, datefmt='%H:%M:%S')
        return formatter.format(record)

class LoggingBase:
    def __init__(self, name: str, level=logging.DEBUG):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        
        if not self.logger.handlers:
            # 1. Terminal Handler (WITH COLORS)
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(ColorFormatter())
            self.logger.addHandler(console_handler)
            
            # 2. File Handler (CLEAN TEXT - NO COLORS)
            # This ensures opensite.log remains human-readable
            file_handler = logging.FileHandler('opensite.log')
            clean_formatter = logging.Formatter(
                '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(clean_formatter)
            self.logger.addHandler(file_handler)

    def debug(self, msg): self.logger.debug(msg)
    def info(self, msg): self.logger.info(msg)
    def warning(self, msg): self.logger.warning(msg)
    def error(self, msg): self.logger.error(msg)