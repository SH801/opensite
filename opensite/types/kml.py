        LogMessage("Downloading KML:     " + feature_name)

        url_basename = basename(dataset['url'])
        kml_file = output_folder + dataset_title + '.kml'
        kmz_file = output_folder + dataset_title + '.kmz'

        if url_basename[-4:] == '.kml':
            attemptDownloadUntilSuccess(dataset['url'], kml_file)
        # If kmz then unzip to folder
        elif url_basename[-4:] == '.kmz':
            attemptDownloadUntilSuccess(dataset['url'], kmz_file)
            with ZipFile(kmz_file, 'r') as zip_ref: zip_ref.extractall(zip_folder)
            os.remove(kmz_file)
        # If zip then download and unzip
        elif url_basename[-4:] == '.zip':
            zip_file = output_folder + dataset_title + '.zip'
            attemptDownloadUntilSuccess(dataset['url'], zip_file)
            with ZipFile(zip_file, 'r') as zip_ref: zip_ref.extractall(zip_folder)
            os.remove(zip_file)
            unzipped_files = getFilesInFolder(zip_folder)
            for unzipped_file in unzipped_files:
                if (unzipped_file[-4:] == '.kmz'):
                    with ZipFile(zip_folder + unzipped_file, 'r') as zip_ref: zip_ref.extractall(zip_folder)

        if isdir(zip_folder):
            unzipped_files = getFilesInFolder(zip_folder)
            for unzipped_file in unzipped_files:
                if (unzipped_file[-4:] == '.kml'):
                    shutil.copy(zip_folder + unzipped_file, kml_file)
            shutil.rmtree(zip_folder)

        if isfile(kml_file):
            # Forced to use togeojson as KML support in ogr2ogr is unpredictable on MacOS
            with open(temp_output_file, "w") as geojson_file:
                    subprocess.call(["togeojson", kml_file], stdout = geojson_file)
            os.remove(kml_file)

        with global_count.get_lock(): global_count.value += 1