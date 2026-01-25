

def initPipeline(command_line):
    """
    Carry out tasks essential to subsequent tasks
    """

    global OSM_DOWNLOADS_FOLDER, OSM_MAIN_DOWNLOAD, BUILD_FOLDER, OSM_BOUNDARIES, OSM_BOUNDARIES_YML, OVERALL_CLIPPING_FILE, WORKING_FOLDER
    global POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, WORKING_CRS
    global PROCESSING_STATE_FILE

    with open(PROCESSING_STATE_FILE, 'w') as file: file.write(command_line)

    postgisDropLegacyTables()

    osm_main_download_path = OSM_DOWNLOADS_FOLDER + basename(OSM_MAIN_DOWNLOAD)
    osm_boundaries_table = reformatTableNameAbsolute(OSM_BOUNDARIES)
    osm_boundaries_osm_export_path = BUILD_FOLDER + OSM_BOUNDARIES
    osm_boundaries_gpkg = osm_boundaries_osm_export_path + '.gpkg'

    # If osm_boundaries_gkpg file does exist, quickly check layers - if no layers (likely that processing was interrupted), getGPKGProjection will delete it
    if isfile(osm_boundaries_gpkg): getGPKGProjection(osm_boundaries_gpkg)

    # If osm_boundaries_gpkg file doesn't exist, carry out OSM download using default OSM specification
    # (before any custom configuration) and run osm-export-tool on osm_boundaries_yml to generate osm_boundaries_gpkg

    if not isfile(osm_boundaries_gpkg):

        osmDownloadData()

        LogMessage("Generating " + basename(osm_boundaries_gpkg) + " from " + basename(OSM_MAIN_DOWNLOAD))

        if not isfile(OSM_BOUNDARIES_YML): LogFatalError("Missing file: " + OSM_BOUNDARIES_YML + ", aborting")

        # Note: 'osm-export-tool' needs path to .gpkg to be output but without .gpkg extension
        runSubprocess([ "osm-export-tool", osm_main_download_path, osm_boundaries_osm_export_path, "-m", OSM_BOUNDARIES_YML])

    osm_boundaries_projection = getGPKGProjection(osm_boundaries_gpkg)

    if not postgisCheckTableExists(osm_boundaries_table):

        LogMessage("Importing into PostGIS: " + basename(osm_boundaries_gpkg))

        scratch_table_1 = '_scratch_table_clipping'
        scratch_table_2 = '_scratch_table_preclipped_boundaries'
        overall_clipping_layer = reformatTableName(OVERALL_CLIPPING_FILE)

        if postgisCheckTableExists(scratch_table_1): postgisDropTable(scratch_table_1)
        if postgisCheckTableExists(scratch_table_2): postgisDropTable(scratch_table_2)

        LogMessage(" --> Step 1: Importing overall clipping layer (dissolved) into scratch table")

        runSubprocess([ "ogr2ogr", \
                        "-f", "PostgreSQL", \
                        'PG:host=' + POSTGRES_HOST + ' user=' + POSTGRES_USER + ' password=' + POSTGRES_PASSWORD + ' dbname=' + POSTGRES_DB, \
                        OVERALL_CLIPPING_FILE, \
                        "-nln", scratch_table_1, \
                        "-nlt", "MULTIPOLYGON", \
                        "-sql", \
                        "SELECT ST_Union(geom) geom FROM 'uk-clipping'", \
                        "--config", "OGR_PG_ENABLE_METADATA", "NO", \
                        "--config", "PG_USE_COPY", "YES" ])

        LogMessage(" --> Step 2: Importing unclipped boundaries into scratch table")

        # Note: clipping on OVERALL_CLIPPING_FILE as some osm boundaries - esp UK nations - are not clipped tightly on coastlines
        runSubprocess([ "ogr2ogr", \
                        "-f", "PostgreSQL", \
                        'PG:host=' + POSTGRES_HOST + ' user=' + POSTGRES_USER + ' password=' + POSTGRES_PASSWORD + ' dbname=' + POSTGRES_DB, \
                        osm_boundaries_gpkg, \
                        "-nln", scratch_table_2, \
                        "-nlt", "MULTIPOLYGON", \
                        "--config", "OGR_PG_ENABLE_METADATA", "NO", \
                        "--config", "PG_USE_COPY", "YES" ])

        LogMessage(" --> Step 3: Clipping partially overlapping polygons")

        postgisExec("""
        CREATE TABLE %s AS 
            SELECT data.fid, data.osm_id, data.name, data.council_name, data.boundary, data.admin_level, ST_Intersection(clipping.geom, data.geom) geom
            FROM %s data, %s clipping 
            WHERE 
                (NOT ST_Contains(clipping.geom, data.geom) AND 
                ST_Intersects(clipping.geom, data.geom));""", \
            (AsIs(osm_boundaries_table), AsIs(scratch_table_2), AsIs(scratch_table_1), ))

        LogMessage(" --> Step 4: Adding fully enclosed polygons")

        postgisExec("""
        INSERT INTO %s  
            SELECT data.fid, data.osm_id, data.name, data.council_name, data.boundary, data.admin_level, data.geom  
            FROM %s data, %s clipping 
            WHERE 
                ST_Contains(clipping.geom, data.geom);""", \
            (AsIs(osm_boundaries_table), AsIs(scratch_table_2), AsIs(scratch_table_1), ))

        if postgisCheckTableExists(scratch_table_1): postgisDropTable(scratch_table_1)
        if postgisCheckTableExists(scratch_table_2): postgisDropTable(scratch_table_2)

        LogMessage(" --> COMPLETED: Processed table: " + osm_boundaries_table)
        LogMessage("------------------------------------------------------------")

        # Once imported, add index to 'name', 'council_name' and 'admin_level' fields
        if postgisCheckTableExists(osm_boundaries_table):
            postgisExec("CREATE INDEX ON %s (name)", (AsIs(osm_boundaries_table), ))
            postgisExec("CREATE INDEX ON %s (council_name)", (AsIs(osm_boundaries_table), ))
            postgisExec("CREATE INDEX ON %s (admin_level)", (AsIs(osm_boundaries_table), ))






def runProcessingOnDownloads(output_folder):
    """
    Processes folder of GeoJSON and GPKG files
    - Adds buffers where appropriate
    - Joins and dissolves child datasets into single parent dataset
    - Joins and dissolves datasets into CKAN groups, one for each group
    - Create single final joined-and-dissolved dataset for entire CKAN database of datasets
    - Converts final files to GeoJSON (EPSG:4326)
    """

    global CUSTOM_CONFIGURATION, CUSTOM_CONFIGURATION_FILE_PREFIX, PROCESSING_START
    global DEBUG_RUN, HEIGHT_TO_TIP, WORKING_CRS, BUILD_FOLDER, OSM_MAIN_DOWNLOAD, OSM_EXPORT_DATA, OSM_BOUNDARIES
    global FINALLAYERS_OUTPUT_FOLDER, FINALLAYERS_CONSOLIDATED, OVERALL_CLIPPING_FILE, REGENERATE_INPUT, REGENERATE_OUTPUT
    global POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
    global OUTPUT_GRID_SPACING, OUTPUT_GRID_TABLE
    global PROCESSING_GRID_SPACING, PROCESSING_GRID_TABLE
    global QGIS_OUTPUT_FILE
    global MAPAPP_FITBOUNDS, MAPAPP_CENTER

    if REGENERATE_INPUT: REGENERATE_OUTPUT = True

    scratch_table_1 = '_scratch_table_1'
    scratch_table_2 = '_scratch_table_2'
    scratch_table_3 = '_scratch_table_3'

    # Prefix all output files with custom_configuration_prefix is CUSTOM_CONFIGURATION set

    custom_configuration_prefix = ''
    if CUSTOM_CONFIGURATION is not None: custom_configuration_prefix = CUSTOM_CONFIGURATION_FILE_PREFIX

    # Ensure all necessary folders exists

    makeFolder(BUILD_FOLDER)
    makeFolder(output_folder)
    makeFolder(FINALLAYERS_OUTPUT_FOLDER)

    # Import OSM-specific data files

    LogMessage("Processing all OSM-specific data layers...")

    custom_prefix = ''
    if CUSTOM_CONFIGURATION is not None: custom_prefix = CUSTOM_CONFIGURATION_FILE_PREFIX

    osm_layers = getOSMLookup()
    osm_export_file = BUILD_FOLDER + custom_prefix + OSM_EXPORT_DATA + '.gpkg'
    osm_export_projection = getGPKGProjection(osm_export_file)

    queue_subprocess = []
    for osm_layer in osm_layers:

        # reformatTableName will add CUSTOM_CONFIGURATION_TABLE_PREFIX to table name if using custom configuration file
        table_name = reformatTableName(osm_layer)
        table_exists = postgisCheckTableExists(table_name)

        if (not REGENERATE_INPUT) and table_exists: continue

        if postgisCheckTableExists(table_name): postgisDropTable(table_name)

        deleteAncestors(table_name)

        # Assume 'osm-export-tool' outputs in EPSG:4326 projection
        import_array = ["ogr2ogr", \
                        "-f", "PostgreSQL", \
                        'PG:host=' + POSTGRES_HOST + ' user=' + POSTGRES_USER + ' password=' + POSTGRES_PASSWORD + ' dbname=' + POSTGRES_DB, \
                        osm_export_file, \
                        "-overwrite", \
                        "-nln", table_name, \
                        "-lco", "GEOMETRY_NAME=geom", \
                        "-lco", "OVERWRITE=YES", \
                        "-dialect", "sqlite", \
                        "-sql", \
                        "SELECT * FROM '" + osm_layer + "'", \
                        "-s_srs", osm_export_projection, \
                        "-t_srs", WORKING_CRS, \
                        "--config", "OGR_PG_ENABLE_METADATA", "NO", \
                        "--config", "PG_USE_COPY", "YES" ]

        queue_subprocess.append(["Importing " + custom_prefix + OSM_EXPORT_DATA + ".gpkg OSM layer into PostGIS: " + osm_layer, import_array])

    multiprocessSubprocess(queue_subprocess)

    LogMessage("Finished processing all OSM-specific data layers")

    # Import overall clipping into PostGIS

    # If custom configuration has 'clipping' defined, use this instead of default overall clipping

    clipping = None
    if CUSTOM_CONFIGURATION is not None:
        if 'clipping' in CUSTOM_CONFIGURATION:
            clipping = CUSTOM_CONFIGURATION['clipping']

            # Convert area names into OSM names
            # For example convert 'northern-ireland' (internal area name) to 'Northern Ireland / Tuaisceart Éireann' (OSM name)
            for clipping_index in range(len(clipping)):
                clipping_current = clipping[clipping_index].lower()
                if clipping_current in OSM_NAME_CONVERT: clipping[clipping_index] = OSM_NAME_CONVERT[clipping_current]
                clipping[clipping_index] = "'" + clipping[clipping_index] + "'"

    # reformatTableName will add CUSTOM_CONFIGURATION_TABLE_PREFIX to table name if using custom configuration file
    # so if using custom config, new copy of clipping_table will be created at CUSTOM_CONFIGURATION_TABLE_PREFIX + 'overall_clipping'

    clipping_table = reformatTableName(OVERALL_CLIPPING_FILE)

    if not postgisCheckTableExists(clipping_table):

        # If CUSTOM_CONFIGURATION requires specific area, modify how we create overall clipping area

        if clipping is None:

            LogMessage("Importing into PostGIS: " + OVERALL_CLIPPING_FILE)

            clipping_file_projection = getGPKGProjection(OVERALL_CLIPPING_FILE)

            runSubprocess([ "ogr2ogr", \
                            "-f", "PostgreSQL", \
                            'PG:host=' + POSTGRES_HOST + ' user=' + POSTGRES_USER + ' password=' + POSTGRES_PASSWORD + ' dbname=' + POSTGRES_DB, \
                            OVERALL_CLIPPING_FILE, \
                            "-overwrite", \
                            "-nln", clipping_table, \
                            "-lco", "GEOMETRY_NAME=geom", \
                            "-lco", "OVERWRITE=YES", \
                            "-s_srs", clipping_file_projection, \
                            "-t_srs", WORKING_CRS, \
                            "--config", "OGR_PG_ENABLE_METADATA", "NO"])

        else:

            osm_boundaries_table = reformatTableNameAbsolute(OSM_BOUNDARIES)

            if not postgisCheckTableExists(osm_boundaries_table): LogFatalError("Essential boundaries table '" + osm_boundaries_table + "' missing - aborting")

            LogMessage("Creating custom clipping boundaries for " + ",".join(clipping) + " from " + osm_boundaries_table)

            postgisExec("CREATE TABLE %s AS SELECT geom FROM %s WHERE (name IN (%s)) OR (council_name IN (%s)) ", (AsIs(clipping_table), AsIs(osm_boundaries_table), AsIs(",".join(clipping)), AsIs(",".join(clipping)), ))
            postgisExec("CREATE INDEX %s ON %s USING GIST (geom);", (AsIs(clipping_table + "_idx"), AsIs(clipping_table), ))

    LogMessage("Checking/creating union of clipping layer - may be default clipping layer or custom clipping layer...")

    clipping_union_table = buildUnionTableName(clipping_table)

    if not postgisCheckTableExists(clipping_union_table):

        LogMessage("Running ST_Union within PostGIS: " + clipping_table + " -> " + clipping_union_table)

        postgisExec("CREATE TABLE %s AS SELECT ST_Union(geom) geom FROM %s", \
                    (AsIs(clipping_union_table), AsIs(clipping_table), ))
        postgisExec("CREATE INDEX %s ON %s USING GIST (geom);", (AsIs(clipping_union_table + "_idx"), AsIs(clipping_union_table), ))

    LogMessage("Finished checking/creating union of clipping layer")

    # If custom clipping, get bounds and centre

    if CUSTOM_CONFIGURATION is not None:
        if 'clipping' in CUSTOM_CONFIGURATION:
            bounds = postgisGetResults("""
            SELECT 
                ST_XMin(ST_Envelope(geom)) AS min_x, 
                ST_YMin(ST_Envelope(geom)) AS min_y,
                ST_XMax(ST_Envelope(geom)) AS max_x,
                ST_YMax(ST_Envelope(geom)) AS max_y
            FROM
                %s;""", (AsIs(clipping_union_table), ))
            bounds = bounds[0]
            MAPAPP_FITBOUNDS = [[bounds[0], bounds[1]], [bounds[2], bounds[3]]]
            MAPAPP_CENTER = [float((bounds[0] + bounds[2]) / 2), float((bounds[1] + bounds[3]) / 2)]

    # Output bounds and center Javascript for use in map app

    outputBoundsAndCenterJavascript()

    # Create output grid

    output_grid = reformatTableName(OUTPUT_GRID_TABLE)

    if not postgisCheckTableExists(output_grid):

        LogMessage("Creating grid overlay to improve mbtiles rendering performance and quality")

        postgisExec("CREATE TABLE %s AS SELECT ST_Transform((ST_SquareGrid(%s, ST_Transform(geom, 3857))).geom, 4326) geom FROM %s;",
                    (AsIs(output_grid), AsIs(OUTPUT_GRID_SPACING), AsIs(clipping_union_table), ))

    # Create processing grid

    processing_grid = reformatTableName(PROCESSING_GRID_TABLE)

    if not postgisCheckTableExists(processing_grid):

        LogMessage("Creating grid overlay to reduce memory load during ST_Union")

        postgisExec("CREATE TABLE %s AS SELECT ST_Transform((ST_SquareGrid(%s, ST_Transform(geom, 3857))).geom, 4326) geom FROM %s;",
                    (AsIs(processing_grid), AsIs(PROCESSING_GRID_SPACING), AsIs(clipping_union_table), ))
        postgisExec("ALTER TABLE %s ADD COLUMN id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY", (AsIs(processing_grid), ))
        postgisExec("DELETE FROM %s WHERE id IN (SELECT grid.id FROM %s grid, %s clipping WHERE ST_Intersects(grid.geom, clipping.geom) IS FALSE);", \
                    (AsIs(processing_grid), AsIs(processing_grid), AsIs(clipping_union_table), ))
        postgisExec("CREATE INDEX %s ON %s USING GIST (geom);", (AsIs(processing_grid + "_idx"), AsIs(processing_grid), ))

    # Populate list of grid square ids

    grid_square_ids = postgisGetResults("SELECT id FROM %s;", (AsIs(processing_grid), ))
    grid_square_ids = [item[0] for item in grid_square_ids]
    grid_square_count = len(grid_square_ids)

    # Import all GeoJSON into PostGIS

    LogMessage("Importing downloaded files into PostGIS...")

    current_datasets = getStructureDatasets()
    downloaded_files = getFilesInFolder(output_folder)

    # Create in-memory list of PostGIS tables and update whenever table dropped to save time
    all_tables = postgisGetAllTables()
    
    queue_index, queue_dict, queue_import = 1, {}, []
    for downloaded_file in downloaded_files:
        queue_index += 1

        core_dataset_name = getCoreDatasetName(downloaded_file)

        # reformatTableName will add CUSTOM_CONFIGURATION_TABLE_PREFIX to table name if using custom configuration file

        imported_table = reformatTableName(core_dataset_name)
        tableexists = (imported_table in all_tables)

        if (not REGENERATE_INPUT) and tableexists: continue

        # If CUSTOM_CONFIGURATION set, only import specific files in custom configuration
        # But typically we import everything in downloads folder

        if CUSTOM_CONFIGURATION is not None:
            if core_dataset_name not in current_datasets: continue

        # If importing dataset, delete import table and all derived files and tables as data may have changed
        if tableexists: 
            postgisDropTable(imported_table)
            all_tables.remove(imported_table)

        # Important to update all_tables (list of active PostGIS tables) in case deleteAncestors drops tables
        all_tables = deleteAncestors(imported_table, all_tables)

        priority = os.path.getsize(join(output_folder, downloaded_file))
        if downloaded_file.endswith('.geojson'): priority = (4 * priority)
        queue_dict_index = getQueueKey(priority, queue_index)
        queue_dict[queue_dict_index] = [downloaded_file, output_folder, imported_table, core_dataset_name]

    if len(queue_dict) != 0:

        queue_dict = dict(sorted(queue_dict.items(), reverse=True))
        queue_datasets = [queue_dict[item] for item in queue_dict]
        chunksize = 1

        multiprocessBefore()

        with Pool(processes=getNumberProcesses()) as p: p.map(importDataset, queue_datasets, chunksize=chunksize)

        multiprocessAfter()

    LogMessage("All downloaded files imported into PostGIS")

    # Add buffers where appropriate to GPKG

    LogMessage("Adding buffers to PostGIS and clipping all tables...")
    LogMessage("------------------------------------------------------------")

    structure_lookup = getStructureLookup()
    groups = structure_lookup.keys()
    parents_lookup = {}

    queue_index, queue_dict = 0, {}
    for group in groups:
        for parent in structure_lookup[group].keys():
            for dataset_name in structure_lookup[group][parent]:
                queue_index += 1
                priority_multiplier = 1
                buffer = getDatasetBuffer(dataset_name)
                orig_table = reformatTableName(dataset_name)
                source_table = reformatTableName(dataset_name)
                processed_table = buildProcessedTableName(source_table)
                if buffer is not None:
                    buffered_table = buildBufferTableName(dataset_name, buffer)
                    processed_table = buildProcessedTableName(buffered_table)
                    source_table = buffered_table
                    # Buffered tables prioritised as inherently more time-consuming
                    priority_multiplier = 100
                parent = getTableParent(source_table)
                if parent not in parents_lookup: parents_lookup[parent] = []
                parents_lookup[parent].append(processed_table)

                priority = priority_multiplier * postgisGetTableSize(orig_table)
                queue_dict_index = getQueueKey(priority, queue_index)
                queue_dict[queue_dict_index] = [queue_index, dataset_name, clipping_union_table, REGENERATE_OUTPUT, HEIGHT_TO_TIP, BLADE_RADIUS, CUSTOM_CONFIGURATION]

    if len(queue_dict) != 0:

        num_datasets_to_process = Value('i', len(queue_dict))
        queue_dict = dict(sorted(queue_dict.items(), reverse=True))
        queue_datasets = [queue_dict[item] for item in queue_dict]
        chunksize = 1

        multiprocessBefore()

        with Pool(processes=getNumberProcesses(), initializer=init_globals_count, initargs=(num_datasets_to_process, )) as p:
            p.map(processDataset, queue_datasets, chunksize=chunksize)

        multiprocessAfter()

    LogMessage("============================================================")
    LogMessage("*** All buffers added to PostGIS and all tables clipped ****")
    LogMessage("============================================================")

    # Amalgamating layers with common 'parents'

    LogMessage("Amalgamating and dissolving layers with common parents...")

    queue_index, finallayers, queue_dict = 0, [], {}
    parents = parents_lookup.keys()
    for parent in parents:
        parent_table = buildFinalLayerTableName(parent)
        finallayers.append(reformatDatasetName(parent_table))
        parent_table_exists = postgisCheckTableExists(parent_table)
        if REGENERATE_OUTPUT or (not parent_table_exists):
            queue_index += 1
            amalgamate_output = "Amalgamating and dissolving children of parent: " + parent
            if parent_table_exists: postgisDropTable(parent_table)
            # Delete any tables and files that are derived from this table
            deleteDatasetAndAncestors(parent_table)
            priority = 0
            for child in parents_lookup[parent]: priority += postgisGetTableSize(child)
            queue_dict_index = getQueueKey(priority, queue_index)
            queue_dict[queue_dict_index] = [queue_index, amalgamate_output, parent_table, parents_lookup[parent], PROCESSING_GRID_TABLE, CUSTOM_CONFIGURATION]

    if len(queue_dict) != 0:

        num_datasets_to_process = Value('i', len(queue_dict))
        queue_dict = dict(sorted(queue_dict.items(), reverse=True))
        queue_datasets = [queue_dict[item] for item in queue_dict]
        chunksize = 1

        multiprocessBefore()

        with Pool(processes=getNumberProcesses(), initializer=init_globals_count, initargs=(num_datasets_to_process, )) as p:
            p.map(postgisAmalgamateAndDissolve, queue_datasets, chunksize=chunksize)

        multiprocessAfter()

    LogMessage("============================================================")
    LogMessage("**** All common parent layers amalgamated and dissolved ****")
    LogMessage("============================================================")

    # Amalgamating datasets by group

    LogMessage("Amalgamating and dissolving layers by group...")

    queue_index, queue_dict = 0, {}
    for group in groups:
        group_items = list((structure_lookup[group]).keys())
        if group_items is None: continue
        group_table = buildFinalLayerTableName(group)
        finallayers.append(reformatDatasetName(group_table))
        group_table_exists = postgisCheckTableExists(group_table)
        group_items.sort()
        if REGENERATE_OUTPUT or (not group_table_exists):
            queue_index += 1
            amalgamate_output = "Amalgamating and dissolving datasets of group: " + group
            # Don't do anything if there is only one element with same name as group
            if (len(group_items) == 1) and (group == group_items[0]): continue
            if group_table_exists: postgisDropTable(group_table)
            # Delete any tables and files that are derived from this table
            deleteDatasetAndAncestors(group_table)
            children = [buildFinalLayerTableName(table_name) for table_name in group_items]
            priority = 0
            for child in children: priority += postgisGetTableSize(child)
            queue_dict_index = getQueueKey(priority, queue_index)
            queue_dict[queue_dict_index] = [queue_index, amalgamate_output, group_table, children, PROCESSING_GRID_TABLE, CUSTOM_CONFIGURATION]

    if len(queue_dict) != 0:

        num_datasets_to_process = Value('i', len(queue_dict))
        queue_dict = dict(sorted(queue_dict.items(), reverse=True))
        queue_datasets = [queue_dict[item] for item in queue_dict]
        chunksize = 1

        multiprocessBefore()

        with Pool(processes=getNumberProcesses(), initializer=init_globals_count, initargs=(num_datasets_to_process, )) as p:
            p.map(postgisAmalgamateAndDissolve, queue_datasets, chunksize=chunksize)

        multiprocessAfter()

    LogMessage("============================================================")
    LogMessage("******* All group layers amalgamated and dissolved *********")
    LogMessage("============================================================")

    # Amalgamating all groups as single layer

    # TODO: Implement multiprocessing version of postgisAmalgamateAndDissolve to improve performance when running it once on final layer

    LogMessage("Amalgamating and dissolving all groups as single overall layer...")

    alllayers_table = buildFinalLayerTableName(FINALLAYERS_CONSOLIDATED)
    final_file_geojson = FINALLAYERS_OUTPUT_FOLDER + custom_configuration_prefix + reformatDatasetName(alllayers_table) + '.geojson'
    final_file_gpkg = FINALLAYERS_OUTPUT_FOLDER + custom_configuration_prefix + reformatDatasetName(alllayers_table) + '.gpkg'
    finallayers.append(reformatDatasetName(alllayers_table))
    alllayers_table_exists = postgisCheckTableExists(alllayers_table)
    if REGENERATE_OUTPUT or (not alllayers_table_exists):
        amalgamate_output = "Amalgamating and dissolving single overall layer: " + FINALLAYERS_CONSOLIDATED
        if alllayers_table_exists: postgisDropTable(alllayers_table)
        children = [buildFinalLayerTableName(table_name) for table_name in groups]
        multiprocessAmalgamateAndDissolve([0, amalgamate_output, alllayers_table, children, PROCESSING_GRID_TABLE])

    LogMessage("============================================================")
    LogMessage("*** All groups amalgamated and dissolved as single layer ***")
    LogMessage("============================================================")

    # Exporting final layers to GeoJSON and GPKG

    LogMessage("Converting final layers to GPKG, SHP and GeoJSON...")

    shp_extensions = ['shp', 'dbf', 'shx', 'prj']

    is_custom_configuration = (CUSTOM_CONFIGURATION is not None)

    # Export from database first...

    filecopy_queue = []
    for finallayer in finallayers:
        finallayer_table = reformatTableName(finallayer)
        core_dataset_name = getFinalLayerCoreDatasetName(finallayer_table)
        latest_name = getFinalLayerLatestName(finallayer_table)
        temp_gpkg = FINALLAYERS_OUTPUT_FOLDER  + 'temp.gpkg'
        finallayer_file_gpkg = FINALLAYERS_OUTPUT_FOLDER + custom_configuration_prefix + finallayer + '.gpkg'

        if isfile(temp_gpkg): os.remove(temp_gpkg)

        # We don't need custom prefix for latest file as it's always just latest
        finallayer_latest_file_gpkg = FINALLAYERS_OUTPUT_FOLDER + latest_name + '.gpkg' 

        if is_custom_configuration or REGENERATE_OUTPUT or (not isfile(finallayer_file_gpkg)):
            LogMessage("Exporting final layer to: " + finallayer_file_gpkg)
            if isfile(finallayer_file_gpkg): os.remove(finallayer_file_gpkg)
            inputs = runSubprocess(["ogr2ogr", \
                            temp_gpkg, \
                            'PG:host=' + POSTGRES_HOST + ' user=' + POSTGRES_USER + ' password=' + POSTGRES_PASSWORD + ' dbname=' + POSTGRES_DB, \
                            "-overwrite", \
                            "-nln", core_dataset_name, \
                            "-nlt", 'POLYGON', \
                            "-dialect", "sqlite", \
                            "-sql", \
                            "SELECT geom geometry FROM '" + finallayer_table + "'", \
                            "-s_srs", WORKING_CRS, \
                            "-t_srs", 'EPSG:4326'])
            checkGPKGIsValid(temp_gpkg, core_dataset_name, inputs)
            # Only copy file to final destination once process has completed - this prevents half-finished files being created
            shutil.copy(temp_gpkg, finallayer_file_gpkg)
            if isfile(temp_gpkg): os.remove(temp_gpkg)

        # Always copy to latest just to be safe
        filecopy_queue.append(["Copying final layer GPKG to: " + finallayer_latest_file_gpkg, finallayer_file_gpkg, finallayer_latest_file_gpkg])

    multiprocessFileCopy(filecopy_queue)

    # Then use ogr2ogr without PostGIS to convert exported GPKG to other formats - GeoJSON and SHP

    filecopy_queue = []
    for finallayer in finallayers:
        finallayer_table = reformatTableName(finallayer)
        core_dataset_name = getFinalLayerCoreDatasetName(finallayer_table)
        latest_name = getFinalLayerLatestName(finallayer_table)
        temp_geojson = FINALLAYERS_OUTPUT_FOLDER + 'temp.geojson'
        temp_shp = FINALLAYERS_OUTPUT_FOLDER + 'temp.shp'
        finallayer_file_gpkg = FINALLAYERS_OUTPUT_FOLDER + custom_configuration_prefix + finallayer + '.gpkg'
        finallayer_file_shp = FINALLAYERS_OUTPUT_FOLDER + custom_configuration_prefix + finallayer + '.shp'
        finallayer_file_geojson = FINALLAYERS_OUTPUT_FOLDER + custom_configuration_prefix + finallayer + '.geojson'

        if isfile(temp_geojson): os.remove(temp_geojson)
        if isfile(temp_shp):
            for shp_extension in shp_extensions:
                temp_individual_shp = FINALLAYERS_OUTPUT_FOLDER + 'temp.' + shp_extension
                if isfile(temp_individual_shp): os.remove(temp_individual_shp)

        # We don't need custom prefix for latest file as it's always just latest
        finallayer_latest_file_shp = FINALLAYERS_OUTPUT_FOLDER + latest_name + '.shp'
        finallayer_latest_file_geojson = FINALLAYERS_OUTPUT_FOLDER + latest_name + '.geojson'

        if is_custom_configuration or REGENERATE_OUTPUT or (not isfile(finallayer_file_shp)):
            LogMessage("Converting final layer GPKG to: " + finallayer_file_shp)
            for shp_extension in shp_extensions:
                if isfile(finallayer_file_shp.replace('shp', shp_extension)): os.remove(finallayer_file_shp.replace('shp', shp_extension))
                if isfile(finallayer_latest_file_shp.replace('shp', shp_extension)): os.remove(finallayer_latest_file_shp.replace('shp', shp_extension))
            if not runSubprocessReturnBoolean(["ogr2ogr", temp_shp, finallayer_file_gpkg]): LogOutOfMemoryAndQuit()

            for shp_extension in shp_extensions:
                temp_individual_shp = FINALLAYERS_OUTPUT_FOLDER + 'temp.' + shp_extension
                shutil.copy(temp_individual_shp, finallayer_file_shp.replace('shp', shp_extension))
                if isfile(temp_individual_shp): os.remove(temp_individual_shp)

        # Always copy to latest just to be safe - can't easily use multiprocessFileCopy as we need SHP to convert to GeoJSON
        LogMessage("Copying final layer SHP to: " + finallayer_latest_file_shp)
        for shp_extension in shp_extensions:
            shutil.copy(finallayer_file_shp.replace('shp', shp_extension), finallayer_latest_file_shp.replace('shp', shp_extension))

        if is_custom_configuration or REGENERATE_OUTPUT or (not isfile(finallayer_file_geojson)):
            LogMessage("Converting final layer SHP to: " + finallayer_file_geojson)
            if isfile(finallayer_file_geojson): os.remove(finallayer_file_geojson)
            # Convert existing output .shp to .geojson to using pyshp streaming to reduce memory load
            convertSHP2GeoJSON(finallayer_latest_file_shp, temp_geojson, core_dataset_name)

            # As we're outputting new geojson, delete corresponding mbtiles file if exists
            finallayer_latest_mbtiles = TILESERVER_DATA_FOLDER + basename(finallayer_latest_file_geojson).replace('.geojson', '.mbtiles')
            if isfile(finallayer_latest_mbtiles): os.remove(finallayer_latest_mbtiles)
            # Only copy file to final destination once process has completed - this prevents half-finished files being processed by mistake
            shutil.copy(temp_geojson, finallayer_file_geojson)
            if isfile(temp_geojson): os.remove(temp_geojson)

        # Always copy to latest just to be safe
        filecopy_queue.append(["Copying final layer GeoJSON to: " + finallayer_latest_file_geojson, finallayer_file_geojson, finallayer_latest_file_geojson])

    multiprocessFileCopy(filecopy_queue)

    LogMessage("All final layers converted to GPKG, SHP and GeoJSON")

    # Build tile server files

    buildTileserverFiles()

    # Build QGIS file

    buildQGISFile()

    processing_time = time.time() - PROCESSING_START
    processing_time_minutes = round(processing_time / 60, 1)
    processing_time_hours = round(processing_time / (60 * 60), 1)
    time_text = str(processing_time_minutes) + " minutes (" + str(processing_time_hours) + " hours) to complete"
    LogMessage("**** Completed processing - " + time_text + " ****")

    run_script = './run-cli.sh'
    if BUILD_FOLDER == 'build-docker/': run_script = './run-docker.sh'

    qgis_text = ''
    if isfile(QGIS_OUTPUT_FILE):
        qgis_text = """QGIS file created at:

\033[1;94m""" + QGIS_OUTPUT_FILE + """\033[0m


"""

    if isfile(final_file_geojson) and isfile(final_file_gpkg):
        print("""
\033[1;34m***********************************************************************
**************** OPEN WIND ENERGY BUILD PROCESS COMPLETE **************
***********************************************************************\033[0m

Final composite layers for turbine height to tip """ + formatValue(HEIGHT_TO_TIP) + """m, blade radius """ + formatValue(BLADE_RADIUS) + """m created at:

\033[1;94m""" + final_file_geojson + """
""" + final_file_gpkg + """\033[0m


To view latest wind constraint layers as map, enter:

\033[1;94m""" + run_script + """\033[0m


""" + qgis_text)

    else:
        LogMessage("ERROR: Failed to created one or more final files")