def outputBoundsAndCenterJavascript():
    """
    Generate Javascript variables MAPAPP_BOUNDS and MAPAPP_CENTER for use in mapapp
    """

    global MAPAPP_JS_BOUNDS_CENTER, MAPAPP_MAXBOUNDS, MAPAPP_FITBOUNDS, MAPAPP_CENTER

    makeFolder(BUILD_FOLDER)
    makeFolder(MAPAPP_FOLDER)

    javascript_content = """
var MAPAPP_MAXBOUNDS = """ + json.dumps(MAPAPP_MAXBOUNDS) + """;
var MAPAPP_FITBOUNDS = """ + json.dumps(MAPAPP_FITBOUNDS) + """;
var MAPAPP_CENTER = """ + json.dumps(MAPAPP_CENTER) + """;"""

    with open(MAPAPP_JS_BOUNDS_CENTER, "w") as javascript_file: javascript_file.write(javascript_content)
