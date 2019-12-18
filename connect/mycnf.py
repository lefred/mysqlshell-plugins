# connect/mycnf.py
# -----------------
# Definition of methods related to old my.cnf file for credentials
#

from mysqlsh import mysql
from mysqlsh import mysqlx


def connect_with_mycnf(use_mysqlx=False, file=None):
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
        session = mysqlx.get_session("%s://%s:%s@%s:%s" % (scheme, user, password, hostname, port))
    else:
        scheme = "mysql"
        port=config['client'].get('port', '3306')
        session = mysql.get_session("%s://%s:%s@%s:%s" % (scheme, user, password, hostname, port))
    
    shell.set_session(session)
    
    return
