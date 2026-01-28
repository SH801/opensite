

def downloadDatasetsSinglePass(ckanurl, output_folder):
    """
    Downloads a CKAN archive and processes the ArcGIS, WFS, GeoJSON and osm-export-tool YML files within it
    TODO: Add support for non-ArcGIS/GeoJSON/WFS/osm-export-tool-YML
    """

    global DOWNLOAD_USER_AGENT
    global CUSTOM_CONFIGURATION, CUSTOM_CONFIGURATION_FILE_PREFIX, OSM_NAME_CONVERT, OVERALL_CLIPPING_FILE
    global REGENERATE_OUTPUT, BUILD_FOLDER, OSM_DOWNLOADS_FOLDER, OSM_MAIN_DOWNLOAD, OSM_CONFIG_FOLDER, WORKING_CRS, OSM_EXPORT_DATA
    global POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

    makeFolder(BUILD_FOLDER)
    makeFolder(OSM_CONFIG_FOLDER)
    makeFolder(output_folder)

    LogMessage("Downloading data catalogue from CKAN " + ckanurl)

    ckanpackages = getCKANPackages(ckanurl)

    generateStructureLookups(ckanpackages)
    generateBufferLookup(ckanpackages)

    # Batch create all OSM layers first
    # Saves time to run osm-export-tool on single file with all datasets

    custom_prefix = ''
    if CUSTOM_CONFIGURATION is not None: custom_prefix = CUSTOM_CONFIGURATION_FILE_PREFIX 

    yaml_all_filename = custom_prefix + 'all.yml'

    osm_layers, yaml_all_content, yaml_all_path = [], {}, OSM_CONFIG_FOLDER + yaml_all_filename
    existing_yaml_content = None
    rerun_osm_export_tool = False


    # Attempt to get projection for osm_export_file - if file is broken, it will be deleted
    if isfile(osm_export_file): osm_export_projection = getGPKGProjection(osm_export_file)

    if not isfile(osm_export_file): rerun_osm_export_tool = True

    osm_layers.sort()
    generateOSMLookup(osm_layers)

    all_datasets_downloaded = Value('i', 1)
    num_datasets_downloaded = Value('i', 0)
    dataset_index, datasets_queue = 0, []




def downloadDataset(dataset_parameters):
    """
    Downloads single dataset
    """

 

    # Produces final GeoJSON/GPKG by converting and applying 'dataset_title' as layer name
    if isfile(temp_output_file):
        if ('.geojson' in temp_output_file):
            reformatGeoJSON(temp_output_file)
            inputs = runSubprocess(["ogr2ogr", "-f", "GeoJSON", "-nln", dataset_title, "-nlt", "GEOMETRY", output_file, temp_output_file])
        if ('.gpkg' in temp_output_file):
            orig_srs = getGPKGProjection(temp_output_file)
            inputs = runSubprocess([ "ogr2ogr", \
                            "-f", "gpkg", \
                            "-nln", dataset_title, \
                            "-nlt", "GEOMETRY", \
                            output_gpkg_file, \
                            temp_output_file, \
                            "-s_srs", orig_srs, \
                            "-t_srs", WORKING_CRS])
        os.remove(temp_output_file)
    
    LogMessage("FINISHED:            " + feature_name)