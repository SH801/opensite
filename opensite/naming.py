# ***********************************************************
# **************** Standardisation functions ****************
# ***********************************************************

def reformatDatasetName(datasettitle):
    """
    Reformats dataset title for compatibility purposes

    - Removes .geojson or .gpkg file extension
    - Replaces spaces with hyphen
    - Replaces ' - ' with double hyphen
    - Replaces _ with hyphen
    - Standardises local variations in dataset names, eg. 'Areas of Special Scientific Interest' (Northern Ireland) -> 'Sites of Special Scientific Interest'
    - For specific very long dataset names, eg. 'Public roads, A and B roads and motorways', shorten as this breaks PostGIS when adding prefixes/suffixes
    - Remove CUSTOM_CONFIGURATION_TABLE_PREFIX and CUSTOM_CONFIGURATION_FILE_PREFIX
    """

    datasettitle = normalizeTitle(datasettitle)
    datasettitle = datasettitle.replace('.geojson', '').replace('.gpkg', '')
    datasettitle = removeCustomConfigurationTablePrefix(datasettitle)
    datasettitle = removeCustomConfigurationFilePrefix(datasettitle)
    reformatted_name = datasettitle.lower().replace(' - ', '--').replace(' ','-').replace('_','-').replace('(', '').replace(')', '')
    reformatted_name = reformatted_name.replace('public-roads-a-and-b-roads-and-motorways', 'public-roads-a-b-motorways')
    reformatted_name = reformatted_name.replace('areas-of-special-scientific-interest', 'sites-of-special-scientific-interest')
    reformatted_name = reformatted_name.replace('conservation-area-boundaries', 'conservation-areas')
    reformatted_name = reformatted_name.replace('scheduled-historic-monument-areas', 'scheduled-ancient-monuments')
    reformatted_name = reformatted_name.replace('priority-habitats--woodland', 'ancient-woodlands')
    reformatted_name = reformatted_name.replace('local-wildlife-reserves', 'local-nature-reserves')
    reformatted_name = reformatted_name.replace('national-scenic-areas-equiv-to-aonb', 'areas-of-outstanding-natural-beauty')
    reformatted_name = reformatted_name.replace('explosive-safeguarded-areas,-danger-areas-near-ranges', 'danger-areas')
    reformatted_name = reformatted_name.replace('separation-distance-to-residential-properties', 'separation-distance-from-residential')

    return reformatted_name

def normalizeTitle(title):
    """
    Converts local variants to use same name
    eg. Areas of Special Scientific Interest -> Sites of Special Scientific Interest
    """
    # **** Add 'canonical_name' into CKAN metadata ****
    title = title.replace('Areas of Special Scientific Interest', 'Sites of Special Scientific Interest')
    title = title.replace('Conservation Area Boundaries', 'Conservation Areas')
    title = title.replace('Scheduled Historic Monument Areas', 'Scheduled Ancient Monuments')
    title = title.replace('Priority Habitats - Woodland', 'Ancient woodlands')
    title = title.replace('National Scenic Areas (equiv to AONB)', 'Areas of Outstanding Natural Beauty')

    return title

def reformatTableNameAbsolute(name):
    """
    Reformats names, eg. dataset names, ignoring custom settings (so absolute) to be compatible with Postgres
    Different from 'reformatTableName' which will add CUSTOM_CONFIGURATION_TABLE_PREFIX if using custom configuration
    """

    return name.replace('.gpkg', '').replace("-", "_")

def reformatTableName(name):
    """
    Reformats names, eg. dataset names, to be compatible with Postgres
    Also adds in CUSTOM_CONFIGURATION_TABLE_PREFIX in case we're using custom configuration file§
    """

    global CUSTOM_CONFIGURATION, CUSTOM_CONFIGURATION_TABLE_PREFIX

    table = reformatTableNameAbsolute(name)

    if CUSTOM_CONFIGURATION is not None:
        if not table.startswith(CUSTOM_CONFIGURATION_TABLE_PREFIX): table = CUSTOM_CONFIGURATION_TABLE_PREFIX + table

    return table

def getDatasetReadableTitle(dataset):
    """
    Gets readable title from dataset internal code
    """

    readabletitle = dataset.strip()
    readabletitle = readabletitle.replace("dcat--", "").replace("mv--", "").replace("fn--", "").replace("--", " _ ").replace("-", " ").replace(" _ ", " - ").capitalize()
    precountry = " - ".join(readabletitle.split(" - ")[:-1])
    country = readabletitle.split(" - ")[-1].title()
    country = country.replace("Uk", "UK")
    if precountry == '': return readabletitle
    return precountry + " - " + country

def buildBufferLayerPath(folder, layername, buffer):
    """
    Builds buffer layer path
    """

    return folder + layername.replace('.gpkg', '') + '--buf-' + buffer + 'm.gpkg'

def buildClippedLayerPath(folder, layername):
    """
    Builds clipped layer path
    """

    return folder + layername.replace('.gpkg', '') + '--clp.gpkg'

def buildBufferTableName(layername, buffer):
    """
    Builds buffer table name
    """

    return reformatTableName(layername) + '__buf_' + buffer.replace(".", "_") + 'm'

def buildProcessedTableName(layername):
    """
    Builds processed table name
    """

    return reformatTableName(layername) + '__pro'

def buildUnionTableName(layername):
    """
    Builds union table name
    """

    return reformatTableName(layername) + '__union'

def removeCustomConfigurationTablePrefix(layername):
    """
    Remove CUSTOM_CONFIGURATION_TABLE_PREFIX if set
    """

    global CUSTOM_CONFIGURATION_TABLE_PREFIX

    custom_configuration_prefix_table_style = CUSTOM_CONFIGURATION_TABLE_PREFIX.replace('-', '_')
    custom_configuration_prefix_dataset_style = CUSTOM_CONFIGURATION_TABLE_PREFIX.replace('_', '-')

    if layername.startswith(custom_configuration_prefix_table_style): layername = layername[len(custom_configuration_prefix_table_style):]
    elif layername.startswith(custom_configuration_prefix_dataset_style): layername = layername[len(custom_configuration_prefix_dataset_style):]

    return layername

def removeCustomConfigurationFilePrefix(layername):
    """
    Remove CUSTOM_CONFIGURATION_FILE_PREFIX if set
    """

    global CUSTOM_CONFIGURATION_FILE_PREFIX

    custom_configuration_prefix_table_style = CUSTOM_CONFIGURATION_FILE_PREFIX.replace('-', '_')
    custom_configuration_prefix_dataset_style = CUSTOM_CONFIGURATION_FILE_PREFIX.replace('_', '-')

    if layername.startswith(custom_configuration_prefix_table_style): layername = layername[len(custom_configuration_prefix_table_style):]
    elif layername.startswith(custom_configuration_prefix_dataset_style): layername = layername[len(custom_configuration_prefix_dataset_style):]

    return layername

def buildTurbineParametersPrefix():
    """
    Builds turbine parameters prefix that is used in table names and output files
    """

    global HEIGHT_TO_TIP, BLADE_RADIUS

    return "tip_" + formatValue(HEIGHT_TO_TIP).replace(".", "_") + "m_bld_" + formatValue(BLADE_RADIUS).replace(".", "_") + "m__"

def buildFinalLayerTableName(layername):
    """
    Builds final layer table name
    Test for whether layer is turbine-height dependent and if so incorporate HEIGHT_TO_TIP and BLADE_RADIUS parameters into name
    """

    dataset_parent = getDatasetParent(layername)
    dataset_parent_no_custom = removeCustomConfigurationTablePrefix(dataset_parent)

    if isTurbineHeightDependent(dataset_parent_no_custom):
        return reformatTableName(buildTurbineParametersPrefix() + reformatTableNameAbsolute(dataset_parent_no_custom))
    return reformatTableName("tip_any__" + reformatTableNameAbsolute(dataset_parent_no_custom))

def getCoreDatasetName(file_path):
    """
    Gets core dataset name from file path
    Core dataset = 'description--location', eg 'national-parks--scotland'
    Remove any 'custom--', 'latest--' or 'tip-..--' prefixes that may have been added to file name
    """

    global CUSTOM_CONFIGURATION, CUSTOM_CONFIGURATION_FILE_PREFIX, LATEST_OUTPUT_FILE_PREFIX

    file_basename = basename(file_path).split(".")[0]

    if CUSTOM_CONFIGURATION is not None:
        if file_basename.startswith(CUSTOM_CONFIGURATION_FILE_PREFIX):
            file_basename = file_basename[len(CUSTOM_CONFIGURATION_FILE_PREFIX):]

    if file_basename.startswith(LATEST_OUTPUT_FILE_PREFIX) or file_basename.startswith('tip-'):
        elements = file_basename.split("--")
        file_basename = "--".join(elements[1:])

    elements = file_basename.split("--")
    return "--".join(elements[0:2])

def getFinalLayerCoreDatasetName(table_name):
    """
    Gets core dataset name from final layer table name
    """

    dataset_name = reformatDatasetName(table_name)
    if dataset_name.startswith('tip'): dataset_name = '--'.join(dataset_name.split('--')[1:])
    return dataset_name

def getFinalLayerLatestName(table_name):
    """
    Gets latest name from table name, eg. 'tip-135m-bld-40m--ecology-and-wildlife...' -> 'latest--ecology-and-wildlife...'
    If CUSTOM_CONFIGURATION, add CUSTOM_CONFIGURATION_FILE_PREFIX
    """

    global LATEST_OUTPUT_FILE_PREFIX, CUSTOM_CONFIGURATION, CUSTOM_CONFIGURATION_FILE_PREFIX

    custom_configuration_prefix = ''
    if CUSTOM_CONFIGURATION is not None: custom_configuration_prefix = CUSTOM_CONFIGURATION_FILE_PREFIX

    dataset_name = reformatDatasetName(table_name)
    elements = dataset_name.split("--")
    if len(elements) > 1: latest_name = custom_configuration_prefix + LATEST_OUTPUT_FILE_PREFIX + "--".join(elements[1:])
    else: latest_name = custom_configuration_prefix + LATEST_OUTPUT_FILE_PREFIX + dataset_name

    return latest_name
