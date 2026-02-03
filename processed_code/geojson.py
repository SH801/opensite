

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

