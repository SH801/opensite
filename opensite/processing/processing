

def multiprocessDivideChunks(queue_dict, chunksize):
    """
    Splits list of queue items into separate chunks so key field, 
    eg. file size, number records, is more evenly shared across chunks
    This means separate processes can start with parallel largest problems first
    """

    queue_dict = dict(sorted(queue_dict.items(), reverse=True))
    queue_datasets_largest_first = [queue_dict[item] for item in queue_dict]
    processes = math.ceil(len(queue_dict) / chunksize)

    queue_index, chunk_items = 0, {}
    for chunk in range(chunksize):
        for process in range(processes):
            if queue_index >= len(queue_datasets_largest_first): break
            chunk_index = chunk + (process * chunksize)
            chunk_items[chunk_index] = queue_datasets_largest_first[queue_index]
            queue_index += 1
        if queue_index >= len(queue_datasets_largest_first): break

    chunk_dict = dict(sorted(chunk_items.items()))
    queue_datasets = [chunk_dict[item] for item in chunk_dict]
    
    if len(queue_dict) != len(queue_datasets):
        LogError("multiprocessDivideChunks: Mismatched counts")
        exit()

    return queue_datasets

def multiprocessBefore():
    """
    Run code before multiprocessing is started
    """

    LogMessage("************************************************")
    LogMessage("********** STARTING MULTIPROCESSING ************")
    LogMessage("************************************************")

def multiprocessAfter():
    """
    Run code after multiprocessing has finished
    """

    LogMessage("************************************************")
    LogMessage("*********** ENDING MULTIPROCESSING *************")
    LogMessage("************************************************")

def singleprocessFileCopy(copy_parameters):
    """
    Single process file copy using copy_parameters
    """

    copy_description, file_src, file_dst = copy_parameters[0], copy_parameters[1], copy_parameters[2]

    LogMessage(copy_description)

    shutil.copy(file_src, file_dst)

def multiprocessFileCopy(queue_files):
    """
    Copies files using multiprocessing to save time
    """

    if len(queue_files) == 0: return
        
    multiprocessBefore()

    chunksize = int(len(queue_files) / multiprocessing.cpu_count()) + 1

    with Pool(processes=getNumberProcesses()) as p: p.map(singleprocessFileCopy, queue_files, chunksize=chunksize)

    multiprocessAfter()

def singleprocessDownload(download_parameters):
    """
    Single process download using download_parameters
    """

    global DOWNLOAD_USER_AGENT

    download_description, url, file_dst = download_parameters[0], download_parameters[1], download_parameters[2]

    LogMessage(download_description)

    opener = urllib.request.build_opener()
    opener.addheaders = [('User-Agent', DOWNLOAD_USER_AGENT)]
    urllib.request.install_opener(opener)
    attemptDownloadUntilSuccess(url, file_dst)

def multiprocessDownload(queue_download):
    """
    Downloads files using multiprocessing to save time
    """

    if len(queue_download) == 0: return

    multiprocessBefore()

    chunksize = int(len(queue_download) / multiprocessing.cpu_count()) + 1

    with Pool(processes=getNumberProcesses()) as p: p.map(singleprocessDownload, queue_download, chunksize=chunksize)

    multiprocessAfter()

def singleprocessSubprocess(subprocess_parameters):
    """
    Single process subprocess using subprocess_parameters
    """

    output_text, subprocess_array = subprocess_parameters[0], subprocess_parameters[1]

    LogMessage("STARTING: " + output_text)

    runSubprocess(subprocess_array)

    LogMessage("FINISHED: " + output_text)

def multiprocessSubprocess(queue_subprocess):
    """
    Runs subprocess using multiprocessing to save time
    """

    if len(queue_subprocess) == 0: return

    multiprocessBefore()

    chunksize = int(len(queue_subprocess) / multiprocessing.cpu_count()) + 1

    with Pool(processes=getNumberProcesses()) as p: p.map(singleprocessSubprocess, queue_subprocess, chunksize=chunksize)

    multiprocessAfter()

def getQueueKey(priority, index):
    """
    Generates dict key from priority and queue index
    """

    # Use float to ensure largest number first; add '9' to prevent loss of '0' in 10, 20...
    return float(str(priority) + "." + str(index) + '9')

def getNumberProcesses():
    """
    Gets number of processes to use in multiprocessing
    If no multiprocessing, then return 1, ie. single process
    """

    global USE_MULTIPROCESSING

    number_processes = 1
    if USE_MULTIPROCESSING: number_processes = None

    return number_processes



# ***********************************************************
# **************** Multiprocessing functions ****************
# ***********************************************************

def init_globals_boolean(global_bool):
    """
    Manages multiprocessing variables - boolean
    """

    global global_boolean
    global_boolean = global_bool

def init_globals_count(global_cnt):
    """
    Manages multiprocessing variables - count
    """

    global global_count
    global_count = global_cnt

def init_globals_boolean_count(global_bool, global_cnt):
    """
    Manages multiprocessing variables - boolean and count
    """

    global global_boolean, global_count
    global_boolean = global_bool
    global_count = global_cnt

def buildQueuePrefix(queue_id):
    """
    Builds queue prefix string for easier tracking in log files 
    """

    return "[QID:" + str(queue_id).zfill(4) + "] "


