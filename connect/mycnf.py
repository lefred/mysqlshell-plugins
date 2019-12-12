# connect/mycnf.py
# -----------------
# Definition of methods related to old my.cnf file for credentials
#

from mysqlsh import mysql


def connect_with_mycnf(mysqlx=False, file=None):
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
    user=config['client'].get('user', getpass.getuser())
    hostname=config['client'].get('host', 'localhost')
    port=config['client'].get('port', '3306')
    password=config['client'].get('password', '')

    if mysqlx:
        scheme = "mysqlx"
    else:
        scheme = "mysql"
    
    import mysqlsh
    shell = mysqlsh.globals.shell
    session = mysql.get_session("%s://%s:%s@%s:%s" % (scheme, user, password, hostname, port))
    shell.set_session(session)
    
    return