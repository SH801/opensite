def rebuildCommandLine(argv):
    """
    Regenerate full command line from list of arguments
    """

    output_args = []
    for arg in argv:
        if ' ' in arg: arg = "'" + str(arg) + "'"
        output_args.append(arg)

    commandline = ' '.join(output_args)
    commandline = commandline.replace('openwindenergy.py', './build-cli.sh')
    
    return commandline



