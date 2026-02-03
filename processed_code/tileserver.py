

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

