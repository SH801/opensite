def getCKANPackages(ckanurl):
    """
    Downloads CKAN archive
    """

    global CUSTOM_CONFIGURATION, CKAN_USER_AGENT

    ckan = RemoteCKAN(ckanurl, user_agent=CKAN_USER_AGENT)
    groups = ckan.action.group_list(id='data-explorer')
    packages = ckan.action.package_list(id='data-explorer')

    selectedgroups = {}
    for package in packages:
        ckan_package = ckan.action.package_show(id=package)

        gpkgfound = False
        arcgisfound = False
        buffer, automation, layer, test = None, None, None, False
        if 'extras' in ckan_package:
            for extra in ckan_package['extras']:
                if extra['key'] == 'buffer': buffer = extra['value']
                if extra['key'] == 'automation': automation = extra['value']
                if extra['key'] == 'layer': layer = extra['value']
                if extra['key'] == 'test': test = True

        if automation == 'exclude': continue
        if automation == 'intersect': continue

        # Prioritise GPKG GeoServices
        for resource in ckan_package['resources']:
            package_link = {'title': ckan_package['title'], 'type': resource['format'], 'url': resource['url'], 'buffer': buffer}
            if resource['format'] == 'GPKG':
                gpkgfound = True
                groups = [group['name'] for group in ckan_package['groups']]
                for group in groups:
                    if group not in selectedgroups: selectedgroups[group] = {}
                    selectedgroups[group][ckan_package['title']] = package_link

        if gpkgfound is False:
            for resource in ckan_package['resources']:
                package_link = {'title': ckan_package['title'], 'type': resource['format'], 'url': resource['url'], 'buffer': buffer}
                if resource['format'] == 'ArcGIS GeoServices REST API':
                    arcgisfound = True
                    groups = [group['name'] for group in ckan_package['groups']]
                    for group in groups:
                        if group not in selectedgroups: selectedgroups[group] = {}
                        selectedgroups[group][ckan_package['title']] = package_link

        # If no ArcGis GeoServices, search for WMS or WMTS
        if (gpkgfound is False) and (arcgisfound is False):
            for resource in ckan_package['resources']:
                resource['format'] = resource['format'].strip()

                package_link = {'title': ckan_package['title'], 'type': resource['format'], 'url': resource['url'], 'buffer': buffer, 'layer': layer}
                if ((resource['format'] == 'GeoJSON') or (resource['format'] == 'WFS') or (resource['format'] == 'osm-export-tool YML') or (resource['format'] == 'KML')):
                    groups = [group['name'] for group in ckan_package['groups']]
                    for group in groups:
                        if group not in selectedgroups: selectedgroups[group] = {}
                        selectedgroups[group][ckan_package['title']] = package_link
                    break

    sorted_groups = sorted(selectedgroups.keys())
    groups = {}

    # Custom configuration allows overriding of groups and datasets we actually use

    custom_groups, custom_buffers, custom_areas, custom_style = None, {}, None, {}
    if CUSTOM_CONFIGURATION is not None: 
        if 'structure' in CUSTOM_CONFIGURATION: custom_groups = CUSTOM_CONFIGURATION['structure']
        if 'buffers' in CUSTOM_CONFIGURATION: custom_buffers = CUSTOM_CONFIGURATION['buffers']
        if 'areas' in CUSTOM_CONFIGURATION: custom_areas = CUSTOM_CONFIGURATION['areas']
        if 'style' in CUSTOM_CONFIGURATION: custom_style = CUSTOM_CONFIGURATION['style']

    for sorted_group in sorted_groups:
        ckan_group = ckan.action.group_show(id=sorted_group)
        color = ''
        if 'extras' in ckan_group:
            for extra in ckan_group['extras']:
                if extra['key'] == 'color': color = extra['value']

        # Allow CUSTOM_CONFIGURATION to override group properties/datasets

        custom_datasets = None
        if custom_groups is not None:
            if reformatDatasetName(sorted_group) not in custom_groups: continue
            custom_datasets = custom_groups[reformatDatasetName(sorted_group)]

        if reformatDatasetName(sorted_group) in custom_style:
            custom_group_style = custom_style[reformatDatasetName(sorted_group)]
            if 'color' in custom_group_style: color = custom_group_style['color']

        groups[sorted_group] = {'title': ckan_group['title'], 'color': color, 'datasets': []}
        sorted_packages = sorted(selectedgroups[sorted_group].keys())
        for sorted_package in sorted_packages:
            dataset = selectedgroups[sorted_group][sorted_package]
            dataset_code = reformatDatasetName(dataset['title'])
            if custom_datasets is not None:
                if dataset_code not in custom_datasets: continue
                if dataset_code in custom_buffers: dataset['buffer'] = custom_buffers[dataset_code]
            if custom_areas is not None:
                dataset_in_customarea = False
                for custom_area in custom_areas:
                    if custom_area in dataset_code: dataset_in_customarea = True
                if not dataset_in_customarea: continue
            groups[sorted_group]['datasets'].append(dataset)

    return groups