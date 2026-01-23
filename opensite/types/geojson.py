def getJSON(json_path):
    """
    Gets contents of JSON file
    """

    with open(json_path, "r") as json_file: return json.load(json_file)


def reformatGeoJSON(file_path):
    """
    Reformats GeoJSON file by removing 'name' attribute which causes problems when querying with sqlite
    """

    if '.geojson' not in basename(file_path): return

    geojson_data = {}
    with open(file_path) as f:
        geojson_data = json.load(f)
        if 'name' in geojson_data: del geojson_data['name']

    with open(file_path, "w") as json_file: json.dump(geojson_data, json_file) 


def checkGeoJSONFile(file_path):
    """
    Checks validity of single GeoJSON file
    """

    file = basename(file_path)

    try:
        json_data = json.load(open(file_path))
        LogMessage("GeoJSON file valid: " + file)
    except:
        LogWarning("GeoJSON file invalid, deleting: " + file)
        os.remove(file_path)
        with global_boolean.get_lock(): global_boolean.value = 0

def checkGeoJSONFiles(output_folder):
    """
    Checks validity of GeoJSON files within folder
    This is required in case download process is interrupted and files are incompletely downloaded
    """

    LogMessage("Checking validity of downloaded GeoJSON files...")

    files = getFilesInFolder(output_folder)

    files_to_check = []
    for file in files:
        if not file.endswith('.geojson'): continue
        file_path = output_folder + file
        files_to_check.append(file_path)

    global_boolean = Value('i', 1)

    multiprocessBefore()

    chunksize = int(len(files_to_check) / multiprocessing.cpu_count()) + 1

    with Pool(processes=getNumberProcesses(), initializer=init_globals_boolean, initargs=(global_boolean,)) as p:
        p.map(checkGeoJSONFile, files_to_check, chunksize=chunksize)

    multiprocessAfter()

    all_valid = (bool)(global_boolean.value)

    if all_valid: LogMessage("All downloaded GeoJSON files valid")

    return all_valid





        LogMessage("Downloading GeoJSON: " + feature_name)

        # Handle non-zipped or zipped version of GeoJSON

        if dataset['url'][-4:] != '.zip':
            attemptDownloadUntilSuccess(dataset['url'], temp_output_file)
        else:
            zip_file = output_folder + dataset_title + '.zip'
            attemptDownloadUntilSuccess(dataset['url'], zip_file)
            with ZipFile(zip_file, 'r') as zip_ref: zip_ref.extractall(zip_folder)
            os.remove(zip_file)

            if isdir(zip_folder):
                unzipped_files = getFilesInFolder(zip_folder)
                for unzipped_file in unzipped_files:
                    if (unzipped_file[-8:] == '.geojson'):
                        shutil.copy(zip_folder + unzipped_file, temp_output_file)
                shutil.rmtree(zip_folder)

        with global_count.get_lock(): global_count.value += 1
