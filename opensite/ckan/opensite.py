import os
import json
import logging
from opensite.constants import OpenSiteConstants
from opensite.ckan.base import CKANBase
from opensite.logging.opensite import OpenSiteLogger
from opensite.download.opensite import OpenSiteDownloader

class OpenSiteCKAN(CKANBase):
    FORMATS =   [
                    'GPKG', 
                    'ArcGIS GeoServices REST API', 
                    'GeoJSON', 
                    'WFS', 
                    'KML',
                    OpenSiteConstants.OSM_YML_FORMAT, 
                    OpenSiteConstants.SITES_YML_FORMAT, 
                ]

    def __init__(self, url: str, apikey: str = None, log_level=logging.INFO):
        super().__init__(url, apikey, log_level)
        self.log = OpenSiteLogger("OpenSiteCKAN", log_level)

    def get_sites(self):
        """Gets all OpenSite sites from CKAN"""
        self.load()
        return self.query([OpenSiteConstants.SITES_YML_FORMAT])
    
    def download_sites(self, sites: list):
        """
        Downloads YML resources matching the provided site names/slugs.
        """

        self.load()
        # Use the constant for 'Open Site Energy YML'
        results = self.query([OpenSiteConstants.SITES_YML_FORMAT])
        
        downloader = OpenSiteDownloader()
        local_paths = []

        self.log.info(f"Searching for site YMLs: {sites}")

        for group_name, data in results.items():
            for dataset in data.get('datasets', []):
                # The 'package_name' is our "crucial bit" (the slug)
                pkg_slug = dataset.get('package_name')

                # Check every matching resource in this dataset
                for res in dataset.get('resources', []):
                    url = res.get('url')
                    basename = os.path.basename(url)
                    # Get 'solar' from 'solar.yml'
                    file_slug = os.path.splitext(basename)[0]

                    # Match if the requested site is the package name OR the filename
                    if pkg_slug in sites or file_slug in sites:
                        self.log.info(f"Match found: '{pkg_slug}' ({basename})")
                        
                        # Use group_name for folder organization
                        path = downloader.get(url, subfolder=group_name, force=True)
                        
                        if path:
                            local_paths.append(str(path))

        # sites may be a list of local YMLs
        for site in sites:
            if ('.yml' in site) and os.path.exists(site):
                local_paths.append(site)
                
        return local_paths
