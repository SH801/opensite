def isSpecificDatasetHeightDependent(dataset_name):
    """
    Returns true or false, depending on whether specific dataset (ignoring children) is turbine-height dependent
    """

    buffer_lookup = getBufferLookup()
    if dataset_name in buffer_lookup:
        buffer_value = str(buffer_lookup[dataset_name])
        if 'height-to-tip' in buffer_value: return True
        if 'blade-radius' in buffer_value: return True
    return False

def isTurbineHeightDependent(dataset_name):
    """
    Returns true or false, depending on whether dataset is turbine-height dependent
    """

    global FINALLAYERS_CONSOLIDATED

    structure_lookup = getStructureLookup()
    dataset_name = reformatDatasetName(dataset_name)

    # We assume overall layer is turbine-height dependent
    if dataset_name == FINALLAYERS_CONSOLIDATED: return True

    children_lookup = {}
    groups = list(structure_lookup.keys())
    for group in groups:
        group_children = list(structure_lookup[group].keys())
        children_lookup[group] = group_children
        for group_child in group_children:
            children_lookup[group_child] = structure_lookup[group][group_child]

    core_dataset_name = getCoreDatasetName(dataset_name)
    alldescendants = getAllDescendants(children_lookup, core_dataset_name)

    for descendant in alldescendants:
        if isSpecificDatasetHeightDependent(descendant): return True
    return False

def getAllDescendants(children_lookup, dataset_name):
    """
    Gets all descendants of dataset
    """

    alldescendants = set()
    if dataset_name in children_lookup:
        for child in children_lookup[dataset_name]:
            alldescendants.add(child)
            descendants = getAllDescendants(children_lookup, child)
            for descendant in descendants:
                alldescendants.add(descendant)
        return list(alldescendants)
    else: return []

def getAllAncestors(dataset_name, include_initial_dataset=True):
    """
    Gets all ancestors of dataset
    """

    global FINALLAYERS_CONSOLIDATED

    # We know FINALLAYERS_CONSOLIDATED is ultimate ancestor of every dataset

    allancestors = [FINALLAYERS_CONSOLIDATED]
    if include_initial_dataset: allancestors.append(dataset_name)

    # Add parent

    parent = getDatasetParent(dataset_name)
    if parent not in allancestors: allancestors.append(parent)

    # Finally check which group grandparent - if any - parent is in

    structure_lookup = getStructureLookup()
    groups = list(structure_lookup.keys())
    for group in groups:
        group_children = list(structure_lookup[group].keys())
        if parent in group_children: allancestors.append(group)

    return allancestors

def getOSMLookup():
    """
    Get OSM lookup JSON
    """

    global OSM_LOOKUP
    return getJSON(OSM_LOOKUP)

def getStructureLookup():
    """
    Get structure lookup JSON
    """

    global STRUCTURE_LOOKUP
    return getJSON(STRUCTURE_LOOKUP)

def getBufferLookup():
    """
    Get buffer lookup JSON
    """

    global BUFFER_LOOKUP
    return getJSON(BUFFER_LOOKUP)

def getStyleLookup():
    """
    Get style lookup JSON
    """

    global STYLE_LOOKUP

    return getJSON(STYLE_LOOKUP)

def getStructureDatasets():
    """
    Gets flat list of all datasets in structure
    """

    structure_lookup = getStructureLookup()
    datasets = []
    for group in structure_lookup.keys():
        for parent in structure_lookup[group].keys():
            for child in structure_lookup[group][parent]: datasets.append(child)

    return datasets

def getDatasetBuffer(datasetname):
    """
    Gets buffer for dataset 'datasetname'
    """

    global HEIGHT_TO_TIP, BLADE_RADIUS

    buffer_lookup = getBufferLookup()
    if datasetname not in buffer_lookup: return None

    try:
        buffer = str(buffer_lookup[datasetname])
        if '* height-to-tip' in buffer:
            # Ideally we have more complex parser to allow complex evaluations
            # but allow 'BUFFER * height-to-tip' for now
            buffer = buffer.replace('* height-to-tip','').strip()
            buffer = HEIGHT_TO_TIP * float(buffer)
        elif '* blade-radius' in buffer:
            # Ideally we have more complex parser to allow complex evaluations
            # but allow 'BUFFER * blade-radius' for now
            buffer = buffer.replace('* blade-radius','').strip()
            buffer = BLADE_RADIUS * float(buffer)
        else:
            buffer = float(buffer)
    except:
        LogFatalError("Problem with buffer value for " + datasetname + ", possible error in configuration file. Is it a single element without '-'?")

    return formatValue(buffer)

def getDatasetParent(file_path):
    """
    Gets dataset parent name from file path
    Parent = 'description', eg 'national-parks' in 'national-parks--scotland'
    """

    file_basename = basename(file_path).split(".")[0]
    return "--".join(file_basename.split("--")[0:1])

def getDatasetParentTitle(title):
    """
    Gets parent of dataset and normalizes specific values
    """

    title = normalizeTitle(title)
    return title.split(" - ")[0]

def getTableParent(table_name):
    """
    Gets table parent name from table name
    Parent = 'description', eg 'national_parks'
    If using custom configuration, table parent will include 
    """

    global CUSTOM_CONFIGURATION

    parent_table = "__".join(table_name.split("__")[0:1])

    if CUSTOM_CONFIGURATION is not None: parent_table = "__".join(table_name.split("__")[0:2])

    return parent_table

def getOutputFileOriginalTable(output_file_path):
    """
    Gets original table used to generate output file
    """

    global HEIGHT_TO_TIP, CUSTOM_CONFIGURATION_FILE_PREFIX

    output_file_basename = basename(output_file_path).split(".")[0]
    original_table_name = reformatTableName(output_file_basename).replace("latest__", "").replace(CUSTOM_CONFIGURATION_FILE_PREFIX.replace("-", "_"), "")

    if 'tip_' not in original_table_name: original_table_name = buildFinalLayerTableName(original_table_name)

    return original_table_name



# *****************************************
# **** Will we need these below? ****
# *****************************************

# ***********************************************************
# ********** Application data structure functions ***********
# ***********************************************************

def generateOSMLookup(osm_data):
    """
    Generates OSM JSON lookup file
    """

    global OSM_LOOKUP

    with open(OSM_LOOKUP, "w") as json_file: json.dump(osm_data, json_file, indent=4)

def generateStructureLookups(ckanpackages):
    """
    Generates structure JSON lookup files including style files for map app
    """

    global CUSTOM_CONFIGURATION, BUILD_FOLDER, MAPAPP_FOLDER, STRUCTURE_LOOKUP, MAPAPP_JS_STRUCTURE, HEIGHT_TO_TIP, BLADE_RADIUS, FINALLAYERS_CONSOLIDATED, TILESERVER_URL

    makeFolder(BUILD_FOLDER)
    makeFolder(MAPAPP_FOLDER)

    structure_lookup = {}
    configuration = ''
    if CUSTOM_CONFIGURATION is not None: configuration = CUSTOM_CONFIGURATION['configuration']

    style_items = [
    {
        "title": "All constraint layers",
        "color": "darkgrey",
        "dataset": getFinalLayerLatestName(FINALLAYERS_CONSOLIDATED),
        "level": 1,
        "children": [],
        "defaultactive": False,
        'height-to-tip': formatValue(HEIGHT_TO_TIP),
        'blade-radius': formatValue(BLADE_RADIUS),
        'configuration': configuration
    }]

    for ckanpackage in ckanpackages.keys():
        ckanpackage_group = reformatDatasetName(ckanpackage)
        structure_lookup[ckanpackage_group] = []
        finallayer_name = getFinalLayerLatestName(ckanpackage_group)
        style_item =   {
                            'title': ckanpackages[ckanpackage]['title'],
                            'color': ckanpackages[ckanpackage]['color'],
                            'dataset': finallayer_name,
                            'level': 1,
                            'defaultactive': True,
                            'height-to-tip': formatValue(HEIGHT_TO_TIP),
                            'blade-radius': formatValue(BLADE_RADIUS)
                        }
        children = {}
        for dataset in ckanpackages[ckanpackage]['datasets']:
            dataset_code = reformatDatasetName(dataset['title'])
            dataset_parent = getDatasetParent(dataset_code)
            if dataset_parent not in children:
                children[dataset_parent] =   {
                                                'title': getDatasetParentTitle(dataset['title']),
                                                'color': ckanpackages[ckanpackage]['color'],
                                                'dataset': getFinalLayerLatestName(dataset_parent),
                                                'level': 2,
                                                'defaultactive': False,
                                                'height-to-tip': formatValue(HEIGHT_TO_TIP),
                                                'blade-radius': formatValue(BLADE_RADIUS)
                                            }
            structure_lookup[ckanpackage_group].append(dataset_code)
        style_item['children'] = [children[children_key] for children_key in children.keys()]
        # If only one child, set parent to only child and remove children
        if len(style_item['children']) == 1:
            style_item = style_item['children'][0]
            style_item['level'] = 1
            style_item['defaultactive'] = True
        style_items.append(style_item)
        structure_lookup[ckanpackage_group] = sorted(structure_lookup[ckanpackage_group])

    structure_hierarchy_lookup = {}
    for ckanpackage in structure_lookup.keys():
        structure_hierarchy_lookup[ckanpackage] = {}
        for dataset in structure_lookup[ckanpackage]:
            layer_parent = "--".join(dataset.split("--")[0:1])
            if layer_parent not in structure_hierarchy_lookup[ckanpackage]: structure_hierarchy_lookup[ckanpackage][layer_parent] = []
            structure_hierarchy_lookup[ckanpackage][layer_parent].append(dataset)

    javascript_content = """
var url_tileserver_style_json = '""" + TILESERVER_URL + """/styles/openwindenergy/style.json';
var openwind_structure = """ + json.dumps({\
        'tipheight': formatValue(HEIGHT_TO_TIP), \
        'bladeradius': formatValue(BLADE_RADIUS), \
        'configuration': configuration, \
        'datasets': style_items\
    }, indent=4) + """;"""

    with open(STRUCTURE_LOOKUP, "w") as json_file: json.dump(structure_hierarchy_lookup, json_file, indent=4)
    with open(STYLE_LOOKUP, "w") as json_file: json.dump(style_items, json_file, indent=4)
    with open(MAPAPP_JS_STRUCTURE, "w") as javascript_file: javascript_file.write(javascript_content)

def generateBufferLookup(ckanpackages):
    """
    Generates buffer JSON lookup file
    """

    global BUFFER_LOOKUP

    buffer_lookup = {}
    for ckanpackage in ckanpackages.keys():
        for dataset in ckanpackages[ckanpackage]['datasets']:
            if 'buffer' in dataset:
                dataset_title = reformatDatasetName(dataset['title'])
                if dataset['buffer'] is not None:
                    buffer_lookup[dataset_title] = dataset['buffer']

    with open(BUFFER_LOOKUP, "w") as json_file: json.dump(buffer_lookup, json_file, indent=4)


