import os
import json
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger
from opensite.cli.opensite import OpenSiteCLI
from opensite.ckan.opensite import OpenSiteCKAN
from opensite.model.tree.opensite import OpenSiteTree

class OpenSiteApplication:
    def __init__(self):
        self._prepare_environment()
        self.log = OpenSiteLogger("OpenSite-App")
        self.log.info("Application initialized")
        self.log_level = OpenSiteConstants.LOGGING_LEVEL

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
        level = os.getenv("OPENSITE_LOG_LEVEL", self.log_level)
        return self.log_level

    def run(self):
        """
        Runs OpenSite application
        """

        # Initialise CLI and get CKAN url
        cli = OpenSiteCLI(log_level=self.log_level) 
        sites = cli.get_sites()
        overrides = cli.get_overrides()

        # Initialize CKAN open data repository to use throughout
        # CKAN may or may not be used to provide site YML configuration
        ckan = OpenSiteCKAN(overrides['ckan'])
        site_ymls = ckan.download_sites(sites)

        # Initialize data model for session
        tree = OpenSiteTree(overrides, log_level=self.log_level)
        tree.add_yamls(site_ymls)
        tree.update_metadata(ckan)

        print(json.dumps(tree.to_list(), indent=4))


    def shutdown(self, message="Process Complete"):
        """Clean exit point for the application."""
        self.log.info(message)