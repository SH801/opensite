def importDataset(dataset_parameters):
    """
    Imports dataset into PostGIS
    """

    downloaded_file, output_folder, imported_table, core_dataset_name = dataset_parameters[0], dataset_parameters[1], dataset_parameters[2], dataset_parameters[3]

    LogMessage("STARTING: Importing into PostGIS: " + downloaded_file)

    downloaded_file_fullpath = output_folder + downloaded_file

    sql_where_clause = None
    orig_srs = 'EPSG:4326'

    if downloaded_file.endswith('.geojson'):

        # Check GeoJSON for crs
        # If missing and in Northern Ireland, then use EPSG:29903
        # If missing and not in Northern Ireland, use EPSG:27700

        json_data = json.load(open(downloaded_file_fullpath))

        if 'crs' in json_data:
            orig_srs = json_data['crs']['properties']['name'].replace('urn:ogc:def:crs:', '').replace('::', ':').replace('OGC:1.3:CRS84', 'EPSG:4326')
        else:
            # DataMapWales' GeoJSON use EPSG:27700 even though default SRS for GeoJSON is EPSG:4326
            if 'wales' in downloaded_file: orig_srs = 'EPSG:27700'
            # Improvement Service GeoJSON uses EPSG:27700
            if 'local-nature-reserves--scotland' in downloaded_file: orig_srs = 'EPSG:27700'

            # Tricky - Northern Ireland could be in correct GeoJSON without explicit crs (so EPSG:4326) or could be incorrect non-EPSG:4326 meters with non GB datum
            if 'northern-ireland' in downloaded_file: orig_srs = 'EPSG:29903'
            # ... so provide exceptions
            if downloaded_file in ['world-heritage-sites--northern-ireland.geojson']: orig_srs = 'EPSG:4326'

        # Historic England Conservation Areas includes 'no data' polygons so remove as too restrictive
        if downloaded_file == 'conservation-areas--england.geojson': sql_where_clause = "Name NOT LIKE 'No data%'"

    # We set CRS=WORKING_CRS during download phase
    if downloaded_file.endswith('.gpkg'): orig_srs = WORKING_CRS

    # Strange bug in ogr2ogr where sometimes fails on GeoJSON with sqlite
    # Therefore avoid using sqlite unless absolutely necessary
    # Don't specify geometry type yet in order to preserve lines and polygons
    subprocess_list = [ "ogr2ogr", \
                        "-f", "PostgreSQL", \
                        'PG:host=' + POSTGRES_HOST + ' user=' + POSTGRES_USER + ' password=' + POSTGRES_PASSWORD + ' dbname=' + POSTGRES_DB, \
                        downloaded_file_fullpath, \
                        "-makevalid", \
                        "-overwrite", \
                        "-lco", "GEOMETRY_NAME=geom", \
                        "-lco", "OVERWRITE=YES", \
                        "-nln", imported_table, \
                        "-nlt", "PROMOTE_TO_MULTI", \
                        "-skipfailures", \
                        "-s_srs", orig_srs, \
                        "-t_srs", WORKING_CRS, \
                        "--config", "PG_USE_COPY", "YES" ]

    if sql_where_clause is not None:
        for extraitem in ["-dialect", "sqlite", "-sql", "SELECT * FROM '" + core_dataset_name + "' WHERE " + sql_where_clause]:
            subprocess_list.append(extraitem)

    for extraconfig in ["--config", "OGR_PG_ENABLE_METADATA", "NO"]: subprocess_list.append(extraconfig)

    runSubprocess(subprocess_list)

    LogMessage("FINISHED: Importing into PostGIS: " + downloaded_file)