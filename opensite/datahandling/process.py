def processDataset(dataset_parameters):
    """
    Process dataset
    """

    global PROCESSING_GRID_TABLE, HEIGHT_TO_TIP, BLADE_RADIUS, CUSTOM_CONFIGURATION

    dataset_id, dataset_name, clipping_union_table, REGENERATE_OUTPUT, HEIGHT_TO_TIP, BLADE_RADIUS, CUSTOM_CONFIGURATION = \
        dataset_parameters[0], dataset_parameters[1], dataset_parameters[2], dataset_parameters[3], dataset_parameters[4], dataset_parameters[5], dataset_parameters[6]

    dataset_id = str(dataset_id).zfill(4)
    prefix = buildQueuePrefix(dataset_id)

    scratch_table_1 = '_scratch_table_1_' + dataset_id
    scratch_table_2 = '_scratch_table_2_' + dataset_id
    scratch_table_3 = '_scratch_table_3_' + dataset_id

    if postgisCheckTableExists(scratch_table_1): postgisDropTable(scratch_table_1)
    if postgisCheckTableExists(scratch_table_2): postgisDropTable(scratch_table_2)
    if postgisCheckTableExists(scratch_table_3): postgisDropTable(scratch_table_3)

    processing_grid = reformatTableName(PROCESSING_GRID_TABLE)
    grid_square_ids = postgisGetResults("SELECT id FROM %s;", (AsIs(processing_grid), ))
    grid_square_ids = [item[0] for item in grid_square_ids]
    grid_square_count = len(grid_square_ids)

    buffer = getDatasetBuffer(dataset_name)
    source_table = reformatTableName(dataset_name)
    processed_table = buildProcessedTableName(source_table)

    with global_count.get_lock(): 
        LogMessage(prefix + "STARTING: Processing: " + source_table + " [" + str(global_count.value) + " dataset(s) to be processed]")

    if buffer is not None:
        buffered_table = buildBufferTableName(dataset_name, buffer)
        processed_table = buildProcessedTableName(buffered_table)
        table_exists = postgisCheckTableExists(buffered_table)
        if REGENERATE_OUTPUT or (not table_exists):
            LogMessage(prefix + "Adding " + buffer + "m buffer: " + source_table + " -> " + buffered_table)
            if table_exists: postgisDropTable(buffered_table)

            # Make special exception for hedgerow as hedgerow polygons represent boundaries that should be buffered as lines
            buffer_polygons_as_lines = False
            if 'hedgerow' in buffered_table: buffer_polygons_as_lines = True

            if buffer_polygons_as_lines:
                postgisExec("""
                CREATE TABLE %s AS 
                (
                    (SELECT ST_Buffer(geom::geography, %s)::geometry geom FROM %s WHERE ST_geometrytype(geom) = 'ST_LineString') UNION 
                    (SELECT ST_Buffer(ST_Boundary(geom)::geography, %s)::geometry geom FROM %s WHERE ST_geometrytype(geom) IN ('ST_Polygon', 'ST_MultiPolygon'))
                );""", \
                    (AsIs(buffered_table), float(buffer), AsIs(source_table), float(buffer), AsIs(source_table), ))
            else:
                postgisExec("CREATE TABLE %s AS SELECT ST_Buffer(geom::geography, %s)::geometry geom FROM %s;", \
                            (AsIs(buffered_table), float(buffer), AsIs(source_table), ))
            postgisExec("CREATE INDEX %s ON %s USING GIST (geom);", (AsIs(buffered_table + "_idx"), AsIs(buffered_table), ))
        source_table = buffered_table

    # Dump original or buffered layer and run processing on it

    processed_table_exists = postgisCheckTableExists(processed_table)
    if REGENERATE_OUTPUT or (not processed_table_exists):
        if processed_table_exists: postgisDropTable(processed_table)

        # Explode geometries with ST_Dump to remove MultiPolygon,
        # MultiSurface, etc and homogenize processing
        # Ideally all dumped tables should contain polygons only (either source or buffered source is (Multi)Polygon)
        # so filter on ST_Polygon

        LogMessage(prefix + source_table + ": Select only polygons, dump and make valid")

        postgisExec("CREATE TABLE %s AS SELECT ST_MakeValid(dumped.geom) geom FROM (SELECT (ST_Dump(geom)).geom geom FROM %s) dumped WHERE ST_geometrytype(dumped.geom) = 'ST_Polygon';", \
                    (AsIs(scratch_table_1), AsIs(source_table), ))

        postgisExec("CREATE INDEX %s ON %s USING GIST (geom);", (AsIs(scratch_table_1 + "_idx"), AsIs(scratch_table_1), ))

        LogMessage(prefix + source_table + ": Clipping partially overlapping polygons")

        postgisExec("""
        CREATE TABLE %s AS 
            SELECT ST_Intersection(clipping.geom, data.geom) geom
            FROM %s data, %s clipping 
            WHERE 
                (NOT ST_Contains(clipping.geom, data.geom) AND 
                ST_Intersects(clipping.geom, data.geom));""", \
            (AsIs(scratch_table_2), AsIs(scratch_table_1), AsIs(clipping_union_table), ))

        LogMessage(prefix + source_table + ": Adding fully enclosed polygons")

        postgisExec("""
        INSERT INTO %s  
            SELECT data.geom  
            FROM %s data, %s clipping 
            WHERE 
                ST_Contains(clipping.geom, data.geom);""", \
            (AsIs(scratch_table_2), AsIs(scratch_table_1), AsIs(clipping_union_table), ))

        LogMessage(prefix + source_table + ": Dumping geometries")

        postgisExec("CREATE TABLE %s AS SELECT (ST_Dump(geom)).geom geom FROM %s;", (AsIs(scratch_table_3), AsIs(scratch_table_2), ))
        postgisExec("CREATE INDEX %s ON %s USING GIST (geom);", (AsIs(scratch_table_3 + "_idx"), AsIs(scratch_table_3), ))

        LogMessage(prefix + source_table + ": Dissolving dataset")

        if postgisCheckTableExists(processed_table): postgisDropTable(processed_table)
        postgisExec("CREATE TABLE %s (id INTEGER, geom GEOMETRY(Polygon, 4326));", (AsIs(processed_table), ))
        postgisExec("CREATE INDEX %s ON %s(id);", (AsIs(processed_table + 'id_idx'), AsIs(processed_table), ))

        for grid_square_index in range(len(grid_square_ids)):
            grid_square_id = grid_square_ids[grid_square_index]

            LogMessage(prefix + source_table + ": Processing grid square " + str(grid_square_index + 1) + "/" + str(grid_square_count))

            postgisExec("""
            INSERT INTO %s 
                SELECT 
                    grid.id, 
                    (ST_Dump(ST_Union(ST_Intersection(grid.geom, dataset.geom)))).geom geom 
                FROM %s grid, %s dataset 
                WHERE grid.id = %s AND ST_geometrytype(dataset.geom) = 'ST_Polygon' GROUP BY grid.id""", (AsIs(processed_table), AsIs(processing_grid), AsIs(scratch_table_3), AsIs(grid_square_id), ))

        postgisExec("CREATE INDEX %s ON %s USING GIST (geom);", (AsIs(processed_table + "_idx"), AsIs(processed_table), ))

        if postgisCheckTableExists(scratch_table_1): postgisDropTable(scratch_table_1)
        if postgisCheckTableExists(scratch_table_2): postgisDropTable(scratch_table_2)
        if postgisCheckTableExists(scratch_table_3): postgisDropTable(scratch_table_3)

    with global_count.get_lock():
        global_count.value -= 1
        LogMessage(prefix + "FINISHED: Processed table: " + processed_table + " [" + str(global_count.value) + " dataset(s) to be processed]")