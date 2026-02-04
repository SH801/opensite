

                    LogMessage("Downloading GPKG:    " + feature_name)

        temp_output_file = temp_base + '.gpkg'

        # Handle non-zipped or zipped version of GPKG

        if not dataset['url'].endswith('.zip'):
            attemptDownloadUntilSuccess(dataset['url'], temp_output_file)
        else:
            zip_file = output_folder + dataset_title + '.zip'
            attemptDownloadUntilSuccess(dataset['url'], zip_file)
            with ZipFile(zip_file, 'r') as zip_ref: zip_ref.extractall(zip_folder)
            os.remove(zip_file)

            if isdir(zip_folder):
                unzipped_files = getFilesInFolder(zip_folder)
                for unzipped_file in unzipped_files:
                    if unzipped_file.endswith('.gpkg'):
                        shutil.copy(zip_folder + unzipped_file, temp_output_file)
                shutil.rmtree(zip_folder)

        with global_count.get_lock(): global_count.value += 1
