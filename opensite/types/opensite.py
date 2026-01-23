

OpenSiteData could contain
- Local parameters
- A 'Data Object Model' that can be traversed
- Ability to 'render' out or 'purge' outputs



def processCustomConfiguration(customconfig):
    """
    Processes custom configuration value
    """

    global CKAN_URL, CKAN_USER_AGENT, CUSTOM_CONFIGURATION_FOLDER, CUSTOM_CONFIGURATION_TABLE_PREFIX
    global OSM_MAIN_DOWNLOAD, OSM_EXPORT_DATA, HEIGHT_TO_TIP, BLADE_RADIUS

    makeFolder(CUSTOM_CONFIGURATION_FOLDER)

    config_downloaded = False
    config_basename = basename(customconfig).lower()
    config_saved_path = CUSTOM_CONFIGURATION_FOLDER + config_basename.replace('.yml', '') + '.yml'
    if isfile(config_saved_path): os.remove(config_saved_path)

    # If '.yml' isn't ending of customconfig, can only be a custom configuration reference on CKAN

    # Open Wind Energy CKAN requires special user-agent for downloads as protection against data crawlers
    opener = urllib.request.build_opener()
    opener.addheaders = [('User-Agent', CKAN_USER_AGENT)]
    urllib.request.install_opener(opener)

    if not config_basename.endswith('.yml'):

        LogMessage("Custom configuration: Attempting to locate '" + config_basename + "' on " + CKAN_URL)

        ckan = RemoteCKAN(CKAN_URL, user_agent=CKAN_USER_AGENT)
        packages = ckan.action.package_list(id='data-explorer')
        config_code = reformatDatasetName(config_basename)

        for package in packages:
            ckan_package = ckan.action.package_show(id=package)

            # Check to see if name of customconfig matches CKAN reformatted package title 

            if reformatDatasetName(ckan_package['title'].strip()) != config_code: continue

            # If matches, search for YML file in resources

            for resource in ckan_package['resources']:
                if ('YML' in resource['format']):
                    attemptDownloadUntilSuccess(resource['url'], config_saved_path)
                    config_downloaded = True
                    break

            if config_downloaded: break

    elif customconfig.startswith('http://') or customconfig.startswith('https://'):
        attemptDownloadUntilSuccess(customconfig, config_saved_path)
        config_downloaded = True

    # Revert user-agent to defaults
    opener = urllib.request.build_opener()
    urllib.request.install_opener(opener)

    if not config_downloaded:
        if isfile(customconfig):
            shutil.copy(customconfig, config_saved_path)
            config_downloaded = True

    if not config_downloaded:

        LogMessage("Unable to access custom configuration '" + customconfig + "'")
        LogMessage(" --> IGNORING CUSTOM CONFIGURATION")

        return None

    yaml_content = None
    with open(config_saved_path) as stream:
        try:
            yaml_content = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            LogFatalError(exc)

    if yaml_content is not None:

        yaml_content['configuration'] = customconfig

        # Dropping all custom configuration tables
        # If we don't do this, things gets very complicated if you start running things across many config files

        LogMessage("Custom configuration: Note all generated tables for custom configuration will have '" + CUSTOM_CONFIGURATION_TABLE_PREFIX + "' prefix")

        LogMessage("Custom configuration: Dropping previous custom configuration database tables")

        postgisDropCustomTables()

    if 'osm' in yaml_content:
        OSM_MAIN_DOWNLOAD = yaml_content['osm']
        LogMessage("Custom configuration: Setting OSM download to " + yaml_content['osm'])

    if 'tip-height' in yaml_content:
        HEIGHT_TO_TIP = float(formatValue(yaml_content['tip-height']))
        LogMessage("Custom configuration: Setting tip-height to " + str(HEIGHT_TO_TIP))

    if 'blade-radius' in yaml_content:
        BLADE_RADIUS = float(formatValue(yaml_content['blade-radius']))
        LogMessage("Custom configuration: Setting blade-radius to " + str(BLADE_RADIUS))

    if 'clipping' in yaml_content:
        LogMessage("Custom configuration: Clipping area(s) [" + ", ".join(yaml_content['clipping']) + "]")

    if 'areas' in yaml_content:
        LogMessage("Custom configuration: Selecting specific area(s) [" + ", ".join(yaml_content['areas']) + "]")

    return yaml_content

def processClippingArea(clippingarea):
    """
    Process custom clipping area
    """

    global CUSTOM_CONFIGURATION, CUSTOM_CONFIGURATION_TABLE_PREFIX

    countries = ['england', 'scotland', 'wales', 'northern-ireland']

    if clippingarea.lower() == 'uk': return CUSTOM_CONFIGURATION # The default setup so change nothing
    if clippingarea.lower().replace(' ', '-') in countries: country = clippingarea.lower()
    else: country = getCountryFromArea(clippingarea)

    if CUSTOM_CONFIGURATION is None: CUSTOM_CONFIGURATION = {'configuration': '--clip ' + clippingarea}
    CUSTOM_CONFIGURATION['clipping'] = [clippingarea]

    if 'areas' not in CUSTOM_CONFIGURATION: CUSTOM_CONFIGURATION['areas'] = [country, 'uk']
    elif country not in CUSTOM_CONFIGURATION: CUSTOM_CONFIGURATION['areas'].append(country)

    LogMessage("Custom clipping area: Clipping on '" + clippingarea + "'")
    LogMessage("Custom clipping area: Selecting country-specific datasets for '" + country + "'")
    LogMessage("Custom clipping area: Note all generated tables for custom configuration will have '" + CUSTOM_CONFIGURATION_TABLE_PREFIX + "' prefix")
    LogMessage("Custom clipping area: Dropping previous custom configuration database tables")
    postgisDropCustomTables()

    return CUSTOM_CONFIGURATION