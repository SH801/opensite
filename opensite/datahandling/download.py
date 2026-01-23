def downloadDatasets(ckanurl, output_folder):
    """
    Repeats download process until all files are valid
    """

    global TEMP_FOLDER

    makeFolder(TEMP_FOLDER)

    while True:

        all_downloaded = downloadDatasetsSinglePass(ckanurl, output_folder)

        if checkGeoJSONFiles(output_folder) and all_downloaded: break

        LogMessage("One or more downloaded files invalid, rerunning download process")

    if isdir(TEMP_FOLDER): shutil.rmtree(TEMP_FOLDER)


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

    osmDownloadData()

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

    # Build list of YML files to download, download using multiprocessing then process all downloads

    queue_download, dataset_titles = [], []
    for ckanpackage in ckanpackages.keys():
        for dataset in ckanpackages[ckanpackage]['datasets']:
            if dataset['type'] != 'osm-export-tool YML': continue

            dataset_title = reformatDatasetName(dataset['title'])
            dataset_titles.append(dataset_title)
            url_basename = basename(dataset['url'])
            downloaded_yml = dataset_title + ".yml"
            downloaded_yml_fullpath = OSM_CONFIG_FOLDER + downloaded_yml
            queue_download.append(["Downloading osm-export-tool YML: " + url_basename + " -> " + downloaded_yml, dataset['url'], downloaded_yml_fullpath])

    multiprocessDownload(queue_download)

    for dataset_title in dataset_titles:
        yaml_content = None
        downloaded_yml = dataset_title + ".yml"
        downloaded_yml_fullpath = OSM_CONFIG_FOLDER + downloaded_yml
        
        with open(downloaded_yml_fullpath) as stream:
            try:
                yaml_content = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                LogMessage(exc)
                exit()

        if yaml_content is None: continue
        yaml_content_keys = list(yaml_content.keys())
        if len(yaml_content_keys) == 0: continue

        # Rename yaml layer with dataset_title and add to aggregate yaml data structure
        yaml_content_firstkey = yaml_content_keys[0]
        yaml_all_content[dataset_title] = yaml_content[yaml_content_firstkey]
        osm_layers.append(dataset_title)

    # Check whether latest yaml matches existing aggregated yaml (if exists)
    # If not, dump out aggregate yaml data structure and process with osm-export-tool

    if isfile(yaml_all_path):
        with open(yaml_all_path, "r") as yaml_file: existing_yaml_content = yaml_file.read()

    # By adding comment specifying which OSM download will be used, we avoid rerunning osm-export-tool unnecessarily
    latest_yaml_content = '# Will be run on ' + OSM_MAIN_DOWNLOAD + '\n\n' + yaml.dump(yaml_all_content)
    if latest_yaml_content != existing_yaml_content:
        rerun_osm_export_tool = True
        with open(yaml_all_path, "w") as yaml_file: yaml_file.write(latest_yaml_content)

    osm_export_base = BUILD_FOLDER + custom_prefix + OSM_EXPORT_DATA
    osm_export_file = osm_export_base + '.gpkg'

    # Attempt to get projection for osm_export_file - if file is broken, it will be deleted
    if isfile(osm_export_file): osm_export_projection = getGPKGProjection(osm_export_file)

    if not isfile(osm_export_file): rerun_osm_export_tool = True

    osm_layers.sort()
    generateOSMLookup(osm_layers)

    all_datasets_downloaded = Value('i', 1)
    num_datasets_downloaded = Value('i', 0)
    dataset_index, datasets_queue = 0, []

    # Add osm-export-tool to datasets queue to be processed with multiprocessing
    # This ensures long-run processes like osm-export-tool start early as possible
    if rerun_osm_export_tool:
        dataset_index += 1
        datasets_queue.append(  {\
                                'subprocess': ["osm-export-tool", OSM_DOWNLOADS_FOLDER + basename(OSM_MAIN_DOWNLOAD), osm_export_base, "-m", yaml_all_path], \
                                'log': ("Running osm-export-tool with aggregated YML '" + yaml_all_filename + "' on: " + basename(OSM_MAIN_DOWNLOAD)) \
                                })

    # Add main downloads to datasets queue to be processed with multiprocessing
    for ckanpackage in ckanpackages.keys():
        for dataset in ckanpackages[ckanpackage]['datasets']:
            dataset_index += 1
            datasets_queue.append(  { \
                                    'dataset_index': dataset_index, \
                                    'dataset': dataset, \
                                    'output_folder': output_folder \
                                    })

    multiprocessBefore()

    LogMessage("Downloading missing datasets or running early-stage data processing...")

    chunksize = 1
    with Pool(processes=getNumberProcesses(), initializer=init_globals_boolean_count, initargs=(all_datasets_downloaded, num_datasets_downloaded, )) as p:
        p.map(downloadDataset, datasets_queue, chunksize=chunksize)
    
    num_downloaded = num_datasets_downloaded.value
    if num_downloaded == 0: LogMessage("All datasets already downloaded")
    else: LogMessage(str(num_downloaded) + " dataset(s) downloaded or processed in this pass")

    multiprocessAfter()

    return (bool)(all_datasets_downloaded.value)

def downloadDataset(dataset_parameters):
    """
    Downloads single dataset
    """

    global CKAN_USER_AGENT, TEMP_FOLDER

    # Check to see if dataset download is really long-running pre-processing step, eg. osm-export-tool
    if 'subprocess' in dataset_parameters:
        LogMessage("STARTING:            " + dataset_parameters['log'])
        runSubprocess(dataset_parameters['subprocess'])
        LogMessage("FINISHED:            " + dataset_parameters['log'])
        with global_count.get_lock(): global_count.value += 1
        return 

    dataset_index = dataset_parameters['dataset_index']
    dataset = dataset_parameters['dataset']
    output_folder = dataset_parameters['output_folder']

    # Don't do anything if osm-export-tool dataset
    if dataset['type'] == 'osm-export-tool YML': return

    dataset_title = reformatDatasetName(dataset['title'])
    feature_name = dataset['title']
    feature_layer_url = dataset['url']
    temp_base = join(TEMP_FOLDER, 'temp_' + str(dataset_index))

    opener = urllib.request.build_opener()
    opener.addheaders = [('User-Agent', CKAN_USER_AGENT)]
    urllib.request.install_opener(opener)

    # Remove any temp files that may have been left if previous run interrupted
    for possible_extension in ['.geojson', '.gml', '.gpkg']:
        if isfile(temp_base + possible_extension): os.remove(temp_base + possible_extension)

    temp_output_file = temp_base + '.geojson'
    output_file = join(output_folder, f'{dataset_title}.geojson')
    output_gpkg_file = join(output_folder, f'{dataset_title}.gpkg')
    zip_folder = output_folder + dataset_title + '/'

    # If export file(s) already exists, quit
    if isfile(output_file) or isfile(output_gpkg_file): return

    LogMessage("STARTING:            " + feature_name)

    if dataset['type'] == 'KML':

        # Use datatypes/kml.py

    elif dataset['type'] == 'WFS':

        # Use datatypes/wfs.py

    elif dataset['type'] == 'GPKG':

        # Use datatypes/gpkg.py

    elif dataset['type'] == 'GeoJSON':

        # Use datatypes/geojson.py

    elif dataset['type'] == "ArcGIS GeoServices REST API":

        # Use datatypes/arcgis.py

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