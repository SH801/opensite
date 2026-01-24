import os
import json
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger
from opensite.cli.opensite import OpenSiteCLI
from opensite.ckan.opensite import OpenSiteCKAN
from opensite.model.graph.opensite import OpenSiteGraph

class OpenSiteApplication:
    def __init__(self, log_level=OpenSiteConstants.LOGGING_LEVEL):
        self._prepare_environment()
        self.log = OpenSiteLogger("OpenSite-App")
        self.log.info("Application initialized")
        self.log_level = log_level

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

        # Initialise CLI
        cli = OpenSiteCLI(log_level=self.log_level) 

        # Initialize CKAN open data repository to use throughout
        # CKAN may or may not be used to provide site YML configuration
        ckan = OpenSiteCKAN(cli.get_current_value('ckan'))
        site_ymls = ckan.download_sites(cli.get_sites())

        # Initialize data model for session
        graph = OpenSiteGraph(cli.get_overrides(), log_level=self.log_level)
        graph.add_yamls(site_ymls)
        graph.update_metadata(ckan)

        # Generate all required processing steps
        graph.explode()

        # Generate graph visualisation
        graph.generate_graph_preview()

        if not cli.get_preview():
            # Don't stop at graph preview but continue onto processing

            # source_nodes = graph.find_nodes_by_props({'node_type': 'source'})
            # print(json.dumps(source_nodes, indent=4))

            print(json.dumps(graph.to_list(), indent=4))


    def shutdown(self, message="Process Complete"):
        """Clean exit point for the application."""
        self.log.info(message)