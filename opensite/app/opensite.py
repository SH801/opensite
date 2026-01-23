import os
import argparse
import logging
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger

class OpenSiteApplication:
    def __init__(self):
        self._prepare_environment()
        self.log = OpenSiteLogger("OpenSite-App")
        self.log.info("Application initialized")
        self._logginglevel = OpenSiteConstants.LOGGING_LEVEL

    def _prepare_environment(self):
        """Creates required system folders defined in constants."""
        folders = [OpenSiteConstants.BUILD_ROOT, OpenSiteConstants.DOWNLOAD_FOLDER]
        for folder in folders:
            if not folder.exists():
                folder.mkdir(parents=True, exist_ok=True)

    def get_loglevel(self):
        """
        Retrieves log level from environment or CLI arguments.
        Defaults to INFO if nothing is provided.
        """
        # Checks shell ENV first, then defaults
        level = os.getenv("OPENSITE_LOG_LEVEL", self._logginglevel)
        return self._logginglevel

    def shutdown(self, message="Process Complete"):
        """Clean exit point for the application."""
        self.log.info(message)