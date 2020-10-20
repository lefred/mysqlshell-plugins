# connect/mycnf.py
# -----------------
# Definition of methods related to old my.cnf file for credentials
#

from mysqlsh.plugin_manager import plugin, plugin_function

from mysqlsh import mysql
from mysqlsh import mysqlx
import re

@plugin_function("legacy_connect.mycnf")
def connect_with_mycnf(use_mysqlx=False, file=None):
    """
    Connect to MySQL using old .my.cnf file.

    This function reads the login information from .my.cnf.

    Args:
        use_mysqlx (bool): Optional boolean value to use or not MySQL X protocol (default: false).
        file: The optional location of the  my.cnf file (default: ~/.my.cnf).

    """

    try:
        import configparser
    except:
        print("ERROR: Module Python configparser must be installed")
        return
    if not file:
        try:
            from pathlib import Path
            file=str(Path.home()) + "/.my.cnf"
        except:
            print("ERROR: Module Pathlib must be installed or provide a filename")
            return
    import getpass

    print("let's use info from %s to connect" % file)
    config = configparser.ConfigParser()
    config.read(file)
    if not config.has_section('client'):
        print("ERROR: your my.cnf file should contain a section '[client]'")
        return

    user=config['client'].get('user', getpass.getuser())
    hostname=config['client'].get('host', 'localhost')
    password=config['client'].get('password', '')

    import mysqlsh
    shell = mysqlsh.globals.shell
    if use_mysqlx:
        scheme = "mysqlx"
        port=config['client'].get('port', '33060')
        connection_dict = { "scheme": scheme, "user": user, "password": password, "host": hostname, "port": port }
        session = mysqlx.get_session(connection_dict)
    else:
        scheme = "mysql"
        port=config['client'].get('port', '3306')
        connection_dict = { "scheme": scheme, "user": user, "password": password, "host": hostname, "port": port }
        session = mysql.get_session(connection_dict)

    shell.set_session(session)

    return
