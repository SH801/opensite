


def makeFolder(folderpath):
    """
    Make folder if it doesn't already exist
    """

    if folderpath.endswith(os.path.sep): folderpath = folderpath[:-1]
    if not isdir(folderpath): makedirs(folderpath)

def getFilesInFolder(folderpath):
    """
    Get list of all files in folder
    Create folder if it doesn't exist
    """

    makeFolder(folderpath)
    files = [f for f in listdir(folderpath) if ((f != '.DS_Store') and (isfile(join(folderpath, f))))]
    if files is not None: files.sort()
    return files

def deleteFolderContentsKeepFolder(folder):
    """
    Deletes contents of folder but keep folder - needed for when docker compose manages folder mappings
    """

    if not isdir(folder): return

    files = getFilesInFolder(folder)
    for file in files: os.remove(folder + file)

    subfolders = [ f.path for f in os.scandir(folder) if f.is_dir() ]

    for subfolder in subfolders:
        subfolder_absolute = os.path.abspath(subfolder)
        if len(subfolder_absolute) < len(folder) or not subfolder_absolute.startswith(folder):
            LogFatalError("Attempting to delete folder outside selected folder, aborting")
        shutil.rmtree(subfolder_absolute)



def attemptDownloadUntilSuccess(url, file_path):
    """
    Keeps attempting download until successful
    """

    while True:
        try:
            urllib.request.urlretrieve(url, file_path)
            return
        except Exception as e:
            LogWarning("Attempt to retrieve " + url + " failed so retrying")
            time.sleep(5)

def attemptGETUntilSuccess(url):
    """
    Keeps attempting GET request until successful
    """

    while True:
        try:
            response = requests.get(url)
            return response
        except Exception as e:
            LogWarning("Attempt to retrieve " + url + " failed so retrying")
            time.sleep(5)

def attemptPOSTUntilSuccess(url, params):
    """
    Keeps attempting POST request until successful
    """

    while True:
        try:
            response = requests.post(url, params)
            return response
        except:
            LogWarning("Attempt to retrieve " + url + " failed so retrying")
            time.sleep(5)

def isfloat(val):
    """
    Checks whether string represents float
    From http://stackoverflow.com/questions/736043/checking-if-a-string-can-be-converted-to-float-in-python
    """
    #If you expect None to be passed:
    if val is None:
        return False
    try:
        float(val)
        return True
    except ValueError:
        return False
    

def formatValue(value):
    """
    Formats float value to be short and readable
    """

    return str(round(value, 1)).replace('.0', '')
