from mysqlsh.plugin_manager import plugin, plugin_function
from support import fetch

@plugin
class support:
    """
    Getting Information useful for requesting help.

    A collection of methods useful when requesting help such as support
    or Community Slack and Forums
    """

@plugin_function("support.fetchInfo")
def get_fetch_info(mysql=True, os=False, advices=False, session=None):
    """
    Fetch info from the system.

    Args:
        mysql (bool): Fetch info from MySQL (enabled by default).
        os (bool): Fetch info from Operating System (requires to have the Shell running locally) This is disabled by default.
        advices (bool): Print eventual advices. This is disabled by default.
        session (object): The session to be used on the operation.
    """

    # Get hold of the global shell object
    import mysqlsh
    shell = mysqlsh.globals.shell

    from datetime import datetime

    output = ""
    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return
    hostname = fetch._get_hostname(session)
    datadirs = fetch._get_datadirs(session)
    header = "Report for %s - %s" % (hostname,datetime.strftime(datetime.now(), "%a %Y-%m-%d %H:%M"))
    print("=" * len(header))
    print(header)
    print("=" * len(header))
    # check if we need to get info related to OS
    if os == True:
        # check if we are running the shell locally
        if fetch._is_local(session, shell):
            # we are connected locally
            osname_sql = fetch._get_os(session)
            osname, name, version, arch, processor, nb_cpu = _get_all_common_os_info()
            if osname_sql == "Linux":
               output2 = fetch._get_all_info_linux(datadirs, advices)
            else:
                print("Your OS (%s) is now yet supported." % osname_sql)

            output += util.output("Operating System", "%s (MySQL built on %s)" % (osname, osname_sql))
            output += util.output("Version", version)
            output += util.output("Architecture", arch)
            output += util.output("Processor", processor)
            output += util.output("CPU Core(s)", nb_cpu)
            output += output2
            print(output)
        else:
            print("For Operating System information you need to run the shell locally")

    if mysql == True:
        output = fetch._get_all_mysql_info(session)
        print(output)
    return
