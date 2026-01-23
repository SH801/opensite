        query_url = f'{feature_layer_url}/query'
        params = {"f": 'json'}
        response = attemptPOSTUntilSuccess(feature_layer_url, params)
        result = json.loads(response.text)
        if 'objectIdField' not in result:
            error_message = feature_name + " - objectIdField missing from response to url: " + feature_layer_url
            if 'error' in result:
                if 'code' in result['error']: error_message = '[' + str(result['error']['code']) + "] " + feature_name + ' - ' + feature_layer_url
            LogError(error_message)
            LogError("Check URL and, if necessary, notify original data provider of potential problem with their data feed")
            with global_boolean.get_lock(): global_boolean.value = 0
            return

        object_id_field = result['objectIdField']

        params = {
            "f": 'json',
            "returnCountOnly": 'true',
            "where": '1=1'
        }

        response = attemptPOSTUntilSuccess(query_url, params)
        result = json.loads(response.text)
        if 'count' not in result: 
            error_message = feature_name + " - 'count' missing from response to url: " + query_url
            if 'error' in result:
                if 'code' in result['error']: error_message = '[' + str(result['error']['code']) + "] " + feature_name + ' - ' + query_url
            LogError(error_message)
            LogError("Check URL and, if necessary, notify original data provider of potential problem with their data feed")
            with global_boolean.get_lock(): global_boolean.value = 0
            return

        no_of_records = result['count']

        LogMessage("Downloading ArcGIS:  " + feature_name + " [records: " + str(no_of_records) + "]")

        records_downloaded = 0
        object_id = -1

        geojson = {
            "type": "FeatureCollection",
            "features": []
        }

        while records_downloaded < no_of_records:
            params = {
                "f": 'geojson',
                "outFields": '*',
                "outSR": 4326, # change the spatial reference if needed (normally GeoJSON uses 4326 for the spatial reference)
                "returnGeometry": 'true',
                "where": f'{object_id_field} > {object_id}'
            }

            firstpass = True

            while True:

                if not firstpass: LogMessage("Attempting to download after first failed attempt: " + query_url)
                firstpass = False

                response = attemptPOSTUntilSuccess(query_url, params)
                result = json.loads(response.text)

                if 'features' not in result:
                    LogWarning("Problem with url, retrying after delay...")
                    time.sleep(5)
                    continue

                if(len(result['features'])):
                    geojson['features'] += result['features']
                    records_downloaded += len(result['features'])
                    object_id = result['features'][len(result['features'])-1]['properties'][object_id_field]
                else:
                    LogWarning("Problem with url, retrying after delay...")
                    time.sleep(5)

                    '''
                        this should not be needed but is here as an extra step to avoid being
                        stuck in a loop if there is something wrong with the service, i.e. the
                        record count stored with the service is incorrect and does not match the
                        actual record count (which can happen).
                    '''
                break

        if(records_downloaded != no_of_records):
            LogMessage("--- ### Note, the record count for the feature layer (" + feature_name + ") is incorrect - this is a bug in the service itself ### ---")

        with open(temp_output_file, 'w') as f:
            f.write(json.dumps(geojson, indent=2))

        with global_count.get_lock(): global_count.value += 1