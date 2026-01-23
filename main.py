import os
import json
import logging
from opensite.app.opensite import OpenSiteApplication
from opensite.cli.opensite import OpenSiteCLI
from opensite.ckan.opensite import OpenSiteCKAN
from opensite.model.tree.opensite import OpenSiteTree

def main():

    print("\n\n\n\n")

    # Initialize application, eg. log level, folders
    app = OpenSiteApplication()
    log_level = app.get_loglevel()

    # Initialise CLI
    cli = OpenSiteCLI(log_level=log_level) 
    sites = cli.get_sites()
    overrides = cli.get_overrides()

    # Initialize data model for session
    tree = OpenSiteTree(overrides, log_level=log_level)

    # Initialize CKAN open data repository we'll be using throughout
    ckan = OpenSiteCKAN(overrides['ckan'])
    site_ymls = ckan.download_sites(sites)

    tree.add_yamls(site_ymls)
    tree.update_metadata(ckan)

    print(json.dumps(tree.to_list(), indent=4))

    # config_path = "wind.yml"

    # if os.path.exists(config_path):
    #     site_tree.load_yaml(config_path)
                
    #     # Exporting to JSON as we just built
    #     # print(json.dumps(site_tree.to_list(), indent=4))
    # else:
    #     print(f"Config not found at {config_path}")

if __name__ == "__main__":
    main()