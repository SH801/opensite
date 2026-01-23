import logging
import sys



def initLogging():
    """
    Initialises logging
    """

    class PaddedProcessFormatter(logging.Formatter):
        def format(self, record):
            # Pad process ID to 4 digits with leading zeros
            record.process_padded = f"PID:{record.process:08d}"
            return super().format(record)

    log_format = '%(asctime)s,%(msecs)03d [%(process_padded)s] [%(levelname)-2s] %(message)s'
    formatter = PaddedProcessFormatter(log_format, "%Y-%m-%d %H:%M:%S")
    handler_1 = logging.StreamHandler()
    handler_2 = logging.FileHandler(LOG_SINGLE_PASS)
    handler_3 = logging.FileHandler("{0}/{1}.log".format(WORKING_FOLDER, datetime.today().strftime('%Y-%m-%d')))

    handler_1.setFormatter(formatter)
    handler_2.setFormatter(formatter)
    handler_3.setFormatter(formatter)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(process_padded)s] [%(levelname)-2s] %(message)s',
        handlers=[handler_1, handler_2, handler_3]
    )

def LogOutOfMemoryAndQuit():
    """
    Logs out of memory message and quits
    """

    LogError("")
    LogError("*** Build failure likely due to lack of memory ***")
    LogError("If running local install, increase swap disk size to > 10Gb")
    LogError("If running Docker install, increase Docker swap size by editing Docker config file:")
    LogError("1. Edit Docker config file - for locations see https://docs.docker.com/desktop/settings-and-maintenance/settings/")
    LogError("2. Modify 'SwapMiB' and set to 10000")
    LogError("3. Fully quit and restart Docker for new 'SwapMiB' setting to take effect")
    LogError("4. Rerun ./build-docker.sh")

    exit()

def LogMessage(logtext):
    """
    Logs message to console with timestamp
    """

    logger = multiprocessing.get_logger()
    logging.info(logtext)

def LogWarning(logtext):
    """
    Logs warning message to console with timestamp
    """

    logger = multiprocessing.get_logger()
    logging.warning(logtext)

def LogError(logtext):
    """
    Logs error message to console with timestamp
    """

    logger = multiprocessing.get_logger()
    logging.error("*** ERROR *** " + logtext)

def LogFatalError(logtext):
    """
    Logs error message to console with timestamp and aborts
    """

    LogError(logtext)
    exit()


class OpenSiteLogger:
    def __init__(self, name="opensite", level="INFO"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level.upper())
        
        # Avoid duplicate handlers if the logger is initialized multiple times
        if not self.logger.handlers:
            self._setup_handlers()

    def _setup_handlers(self):
        # Create a console handler
        console_handler = logging.StreamHandler(sys.stdout)
        
        # Define a professional format
        # [opensite] 2026-01-22 16:53: INFO: message
        formatter = logging.Formatter(
            '[%(name)s] %(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(console_handler)

    def info(self, msg):
        self.logger.info(msg)

    def error(self, msg):
        self.logger.error(msg)

    def warn(self, msg):
        self.logger.warning(msg)

    def debug(self, msg):
        self.logger.debug(msg)