# support/fetch.py
# -----------------
# Definition of member functions for the support extension object to fetch info
# that can be useful when seeking for help
#

import os as operatingsystem
from platform import release, version
from re import A
import subprocess
from datetime import datetime
from support.sections import *
from mysqlsh.plugin_manager import plugin, plugin_function
from support.sections import innodb

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

def _is_local(session, shell, branch):
    if branch == "80":
        stmt = "select @@hostname, @@port, @@mysqlx_port"
    else:
        stmt = "select @@hostname, @@port, ''"
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

def _get_version_branch(session):
    stmt = """select @@version"""
    result = session.run_sql(stmt)
    row = result.fetch_one()
    major, minor, release = row[0].split(".")
    if "-" in release:
        release, *garbage = release.split("-")
    branch = "{}{}".format(major, minor)
    return branch, release

def _get_datadirs(session, branch):
    stmt = """select @@datadir,@@innodb_data_home_dir,@@innodb_log_group_home_dir,
              @@innodb_temp_tablespaces_dir, @@innodb_tmpdir, @@tmpdir"""
    if branch == "56":
        stmt = """select @@datadir,@@innodb_data_home_dir,@@innodb_log_group_home_dir"""
    else:
        if branch == "57":
            stmt = """select @@datadir,@@innodb_data_home_dir,@@innodb_log_group_home_dir,
              @@innodb_tmpdir, @@tmpdir"""

    result = session.run_sql(stmt)
    row = result.fetch_one_object()
    return row

def _get_processor_name():
    if platform.system() == "Windows":
        return platform.processor()
    elif platform.system() == "Darwin":
        operatingsystem.environ['PATH'] = operatingsystem.environ['PATH'] + operatingsystem.pathsep + '/usr/sbin'
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
    nb_cpu = operatingsystem.cpu_count()
    osname, name, version, _, _, arch = platform.uname()
    processor = _get_processor_name()
    return osname, name, version, arch, processor.lstrip(' '), nb_cpu

def _get_all_info_linux(datadirs, advices):
    output = memory.get_linux_memory_usage(advices)
    output += disks.get_linux_disk_info(datadirs)
    return output

def _get_all_mysql_info(session, advices, details):
    supported, output, version = mysql.version_info(session)
    branch, releasever = _get_version_branch(session)
    if not supported:
        output += util.print_orange("Your MySQL version might be old or not fully supported by this tool!")
    
    # get all dataset
    output += mysql.get_dataset(session, branch)
    output += "\n"
    output += mysql.get_engines(session, advices)
    output += "\n"
    output += mysql.get_largest_innodb_tables(session, branch, details)
    output += "\n"
    output += mysql.get_tables_without_pk(session, advices, details)
    output += "\n"
    output += innodb.get_innodb_info(session, advices, branch, releasever)
    output += "\n"
    if branch == "80":
        output += mysql.get_configured_variables(session, details)
        output += "\n"
    output += replication.get_replication_info(session, advices, branch, releasever)
    output += "\n"
    output += mysql.get_flush_commands(session, advices)
    output += "\n"
    output += mysql.get_tables_in_mysql(session, branch, advices, details)
    output += mysql.get_routines(session, advices, details)
    output += "\n"
    output += mysql.get_users_auth_plugins(session, advices)
    output += "\n"
    output += mysql.get_users_privileges(session, advices)
    if advices:
        major, minor, release = version.split(".")
        if "-" in release:
            release, *garbage = release.split("-")
        if major == 5 and minor < 7:
            output += util.print_red("For MDS Inbound Replication, you need to have at least 5.7.9")
        if major == 5 and minor == 7 and release < 9:
            output += util.print_orange("For MDS Inbound Replication, you need to have at least 5.7.9")
    output += "\n"
    output += hosts.get_host_info(session, advices, details, branch)
    output += keywords.check_reserved_keywords(session, advices, details)

    return output

@plugin_function("support.fetchInfo")
def get_fetch_info(mysql=True, os=False, advices=False, details=False, session=None):
    """
    Fetch info from the system.

    Args:
        mysql (bool): Fetch info from MySQL (enabled by default).
        os (bool): Fetch info from Operating System (requires to have the Shell running locally) This is disabled by default.
        advices (bool): Print eventual advices. This is disabled by default.
        details (bool): Print eventual details when possible. This is disabled by default.
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
    branch, releasever = _get_version_branch(session)
    hostname = _get_hostname(session)
    datadirs = _get_datadirs(session, branch)
    header = "Report for %s - %s" % (hostname,datetime.strftime(datetime.now(), "%a %Y-%m-%d %H:%M"))
    print("=" * len(header))
    print(header)
    print("=" * len(header))
    # check if we need to get info related to OS
    if os == True:
        output2 = ""
        # check if we are running the shell locally
        if _is_local(session, shell, branch):
            # we are connected locally
            osname_sql = _get_os(session)
            osname, name, version, arch, processor, nb_cpu = _get_all_common_os_info()
            if osname_sql == "Linux":
               output2 = _get_all_info_linux(datadirs, advices)
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
        output = _get_all_mysql_info(session, advices, details)
        print(output)
        answer = shell.prompt(
            'Do you want to save the ouptut to file ? (Y/n) ', {'defaultValue': 'y'})
        if answer.lower() == 'y':
            outfile = "~/system_info_{}_{}.txt".format(hostname, datetime.strftime(datetime.now(), "%Y-%m-%d_%H-%M-%S"))
            outfile = operatingsystem.path.expanduser(outfile)
            f_output = open("{}".format(outfile), 'a')
            f_output.write(output)
            f_output.close()
            print("Report has been saved in {}".format(outfile))
    return
