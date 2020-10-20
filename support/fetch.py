# support/fetch.py
# -----------------
# Definition of member functions for the support extension object to fetch info
# that can be useful when seeking for help
#

import os
import subprocess
from datetime import datetime
from support.sections import *

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
