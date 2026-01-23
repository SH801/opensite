def getGPKGProjection(file_path):
    """
    Gets projection in GPKG
    """

    if isfile(file_path):
        with sqlite3.connect(file_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("select a.srs_id from gpkg_contents as a;")
            result = cursor.fetchall()
            if len(result) == 0:
                LogMessage(file_path + " has no layers - deleting")
                os.remove(file_path)
                return None
            else:
                firstrow = result[0]
                return 'EPSG:' + str(dict(firstrow)['srs_id'])

def checkGPKGIsValid(file_path, layer_name, inputs):
    """
    Checks whether GPKG has correct layer name
    """

    if isfile(file_path):
        with sqlite3.connect(file_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
            select
                    a.table_name, a.data_type, a.srs_id,
                    b.column_name, b.geometry_type_name,
                    c.feature_count
            from gpkg_contents as a
            left join gpkg_geometry_columns as b
                    on a.table_name = b.table_name
            left join gpkg_ogr_contents as c
                    on a.table_name = c.table_name
            ;
            """)
            result = cursor.fetchall()
            if len(result) == 0:
                LogError(file_path + " has no layers - aborting")
                # os.remove(file_path)
                LogError("Reproduce error by manually entering:\n" + inputs)
                LogFatalError("*** Error may be due to lack of memory (increase memory and retry) or corrupt PostGIS table (delete table and rerun) ***")
            else:
                firstrow = dict(result[0])
                if firstrow['table_name'] != layer_name:
                    LogError(file_path + " does not have first layer " + layer_name + " - aborting")
                    print(len(result), json.dumps(firstrow, indent=4))
                    # os.remove(file_path)
                    LogError("Reproduce error by manually entering:\n" + inputs)
                    LogFatalError("*** Error may be due to lack of memory (increase memory and retry) or corrupt PostGIS table (delete table and rerun) ***")
                return True
            


                    LogMessage("Downloading GPKG:    " + feature_name)

        temp_output_file = temp_base + '.gpkg'

        # Handle non-zipped or zipped version of GPKG

        if not dataset['url'].endswith('.zip'):
            attemptDownloadUntilSuccess(dataset['url'], temp_output_file)
        else:
            zip_file = output_folder + dataset_title + '.zip'
            attemptDownloadUntilSuccess(dataset['url'], zip_file)
            with ZipFile(zip_file, 'r') as zip_ref: zip_ref.extractall(zip_folder)
            os.remove(zip_file)

            if isdir(zip_folder):
                unzipped_files = getFilesInFolder(zip_folder)
                for unzipped_file in unzipped_files:
                    if unzipped_file.endswith('.gpkg'):
                        shutil.copy(zip_folder + unzipped_file, temp_output_file)
                shutil.rmtree(zip_folder)

        with global_count.get_lock(): global_count.value += 1
