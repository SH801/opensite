def installTileserverFonts():
    """
    Installs fonts required for tileserver-gl
    """

    global BUILD_FOLDER, TILESERVER_FOLDER

    LogMessage("Attempting tileserver fonts installation...")

    tileserver_font_folder = TILESERVER_FOLDER + 'fonts/'

    if BUILD_FOLDER == 'build-docker/':

        # On docker openwindenergy-fonts container copies fonts to 'fonts/' folder
        # So need to wait for it to finish this

        while True:
            if isdir(tileserver_font_folder):
                LogMessage("Tileserver fonts folder already exists - SUCCESS")
                return True
            time.sleep(5)

    else:

        # Server build clones fonts from https://github.com/open-wind/openmaptiles-fonts.git
        if isdir(tileserver_font_folder): 
            LogMessage("Tileserver fonts folder already exists - SUCCESS")
            return True

        # Download tileserver fonts

        if not isdir(basename(TILESERVER_FONTS_GITHUB)):

            LogMessage("Downloading tileserver fonts")

            inputs = runSubprocess(["git", "clone", TILESERVER_FONTS_GITHUB])

        working_dir = os.getcwd()
        os.chdir(basename(TILESERVER_FONTS_GITHUB))

        LogMessage("Generating PBF fonts")

        if not runSubprocessReturnBoolean(["npm", "install"]):
            os.chdir(working_dir)
            return False

        if not runSubprocessReturnBoolean(["node", "./generate.js"]):
            os.chdir(working_dir)
            return False

        os.chdir(working_dir)

        LogMessage("Copying PBF fonts to tileserver folder")

        tileserver_font_folder_src = basename(TILESERVER_FONTS_GITHUB) + '/_output'

        shutil.copytree(tileserver_font_folder_src, tileserver_font_folder)

        return True