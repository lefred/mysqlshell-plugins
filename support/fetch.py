# support/fetch.py
# -----------------
# Definition of member functions for the support extension object to fetch info
# that can be useful when seeking for help
#

import os
import subprocess
from datetime import datetime
from support.sections import *
from mysqlsh.plugin_manager import plugin, plugin_function

try:
    import platform
except:
    print("Error importing module platform, check if it's installed")
    exit
try:
    import re
except:
    print("Error importing module re, check if it's installed")
    exit

def _is_local(session, shell):
    stmt = "select @@hostname, @@port, @@mysqlx_port"
    result = session.run_sql(stmt)
    row = result.fetch_one()
    hostname = row[0]
    port = row[1]
    xport = row[2]
    uri_json = shell.parse_uri(session.get_uri())
    if uri_json['host'] == "localhost" and ( port == uri_json['port'] or xport == uri_json['port']):
        return True
    if uri_json['host'] == "127.0.0.1" and ( port == uri_json['port'] or xport == uri_json['port']) :
        return True
    if uri_json['host'] == hostname:
        return True
    return False

def _get_os(session):
    stmt = "select @@version_compile_os"
    result = session.run_sql(stmt)
    row = result.fetch_one()
    return row[0]

def _get_hostname(session):
    stmt = "select @@hostname"
    result = session.run_sql(stmt)
    row = result.fetch_one()
    return row[0]

def _get_datadirs(session):
    stmt = """select @@datadir,@@innodb_data_home_dir,@@innodb_log_group_home_dir,
              @@innodb_temp_tablespaces_dir, @@innodb_tmpdir, @@tmpdir"""
    result = session.run_sql(stmt)
    row = result.fetch_one_object()
    return row

def _get_processor_name():
    if platform.system() == "Windows":
        return platform.processor()
    elif platform.system() == "Darwin":
        os.environ['PATH'] = os.environ['PATH'] + os.pathsep + '/usr/sbin'
        command ="sysctl -n machdep.cpu.brand_string"
        return subprocess.check_output(command).strip()
    elif platform.system() == "Linux":
        command = "cat /proc/cpuinfo"
        all_info = subprocess.check_output(command, shell=True).strip()
        for line in all_info.decode("utf-8").split("\n"):
            if "model name" in line:
                return re.sub( ".*model name.*:", "", line,1)
    return ""

def _get_all_common_os_info():
    nb_cpu = os.cpu_count()
    osname, name, version, _, _, arch = platform.uname()
    processor = _get_processor_name()
    return osname, name, version, arch, processor.lstrip(' '), nb_cpu

def _get_all_info_linux(datadirs, advices):
    output = memory.get_linux_memory_usage(advices)
    output += disks.get_linux_disk_info(datadirs)
    return output

def _get_all_mysql_info(session):
    supported, output = mysql.version_info(session)
    if not supported:
        util.print_red("Your MySQL version is not supported !")
    else:
        # get all dataset
        output += mysql.get_dataset(session)
        output += mysql.get_largest_innodb_tables(session)
        output += mysql.get_tables_without_pk(session)
    return output

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
