

def getCountryFromArea(area):
    """
    Determine country that area is in using OSM_BOUNDARIES_GPKG
    """

    global OSM_BOUNDARIES
    global POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, WORKING_CRS
    global OSM_NAME_CONVERT

    osm_boundaries_table = reformatTableNameAbsolute(OSM_BOUNDARIES)
    countries = [OSM_NAME_CONVERT[country] for country in OSM_NAME_CONVERT.keys()]

    results = postgisGetResults("""
    WITH primaryarea AS
    (
        SELECT geom FROM %s WHERE (name = %s) OR (council_name = %s) LIMIT 1
    )
    SELECT 
        name, 
        ST_Area(ST_Intersection(primaryarea.geom, secondaryarea.geom)) geom_intersection 
    FROM %s secondaryarea, primaryarea 
    WHERE name = ANY (%s) AND ST_Intersects(primaryarea.geom, secondaryarea.geom) ORDER BY geom_intersection DESC LIMIT 1;
    """, (AsIs(osm_boundaries_table) , area, area, AsIs(osm_boundaries_table), countries, ))

    containing_country = results[0][0]

    for canonical_country in OSM_NAME_CONVERT.keys():
        if OSM_NAME_CONVERT[canonical_country] == containing_country: return canonical_country

    return None

