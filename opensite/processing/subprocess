


def subprocessGetLayerName(subprocess_array):
    """
    Gets layer name from subprocess array
    """

    for index in range(len(subprocess_array)):
        if subprocess_array[index] == '-nln': return subprocess_array[index + 1].replace("-", "_")

    return None

def runSubprocessWithEnv(subprocess_array, env):
    """
    Runs subprocess with environment variables
    """

    output = subprocess.run(subprocess_array, env=env)

    # print("\n" + " ".join(subprocess_array) + "\n")

    if output.returncode != 0: LogFatalError("subprocess.run failed with error code: " + str(output.returncode) + '\n' + " ".join(subprocess_array))
    return " ".join(subprocess_array)

def runSubprocess(subprocess_array):
    """
    Runs subprocess
    """

    global SERVER_BUILD, USE_MULTIPROCESSING

    if (not SERVER_BUILD) and (not USE_MULTIPROCESSING):
        if subprocess_array[0] == 'ogr2ogr': subprocess_array.append('-progress')

    output = subprocess.run(subprocess_array)

    # print("\n" + " ".join(subprocess_array) + "\n")

    if output.returncode != 0: LogFatalError("subprocess.run failed with error code: " + str(output.returncode) + '\n' + " ".join(subprocess_array))
    return " ".join(subprocess_array)

def runSubprocessReturnBoolean(subprocess_array):
    """
    Runs subprocess and returns True or False depending on whether successful or not
    """

    global SERVER_BUILD

    if not SERVER_BUILD:
        if subprocess_array[0] == 'ogr2ogr': subprocess_array.append('-progress')

    output = subprocess.run(subprocess_array)

    # print("\n" + " ".join(subprocess_array) + "\n")

    if output.returncode == 0: return True

    return False

def runSubprocessAndOutput(subprocess_array):
    """
    Runs subprocess and prints output of process
    """

    output = subprocess.run(subprocess_array, capture_output=True, text=True)

    LogMessage(output.stdout.strip())

    if output.returncode != 0: LogFatalError("subprocess.run failed with error code: " + str(output.returncode) + '\n' + " ".join(subprocess_array))


