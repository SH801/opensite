import os
import logging
from pathlib import Path

class OpenSiteConstants:
    """Arbitrary application constants that don't change per environment."""

    # Default logging level for entire application
    LOGGING_LEVEL           = logging.DEBUG

    # Format text used by CKAN to indicate osm-export-tool YML file
    OSM_YML_FORMAT          = "osm-export-tool YML"

    # Format text used by CKAN to indicate Open Site Energy YML file
    SITES_YML_FORMAT        = "Open Site Energy YML"
    
    # CKAN formats we can accept
    CKAN_FORMATS            = \
                            [
                                'GPKG', 
                                'ArcGIS GeoServices REST API', 
                                'GeoJSON', 
                                'WFS', 
                                'KML',
                                OSM_YML_FORMAT, 
                                SITES_YML_FORMAT, 
                            ]

    # File extensions we should expect from downloading these different CKAN formats
    CKAN_FILE_EXTENSIONS     = \
                            {
                                'GPKG' : 'gpkg', 
                                'ArcGIS GeoServices REST API': 'geojson', 
                                'GeoJSON': 'geojson', 
                                'WFS': 'geojson', 
                                'KML': 'geojson',
                                OSM_YML_FORMAT: 'yml', 
                                SITES_YML_FORMAT: 'yml', 
                            }

    # Root build directory
    BUILD_ROOT              = Path(os.getenv("BUILD_FOLDER", "build"))
    
    # Sub-directories
    DOWNLOAD_FOLDER         = BUILD_ROOT / "downloads"
    CACHE_FOLDER            = BUILD_ROOT / "cache"
    LOG_FOLDER              = BUILD_ROOT / "logs"

    # Acceptable CLI properties
    TREE_BRANCH_PROPERTIES  = \
                            {
                                'functions':    [
                                                    'height-to-tip', 
                                                    'blade-radius'
                                                ],
                                'default':      [
                                                    'title', 
                                                    'type', 
                                                    'clipping-path', 
                                                    'osm',
                                                    'ckan'
                                                ]
                            }
