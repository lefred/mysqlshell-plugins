from mysqlsh.plugin_manager import plugin, plugin_function
from mysqlsh_plugins_common import run_and_show
import time

@plugin_function("logs.getErrorLogConfig")
def get_error_log_config(session=None):
    """
    Prints the values of the configuration variables related
    to the Error Log.

    Args:
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    """
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    stmt = """select variable_name, variable_value from  performance_schema.global_variables
              where VARIABLE_NAME like 'log_error%'"""

    result = session.run_sql(stmt)
    rows = result.fetch_all()
    fmt = "{0:26s} : {1:120s}"
    print("MySQL Error Log Settings:")
    print("-------------------------")
    for row in rows:
        print(fmt.format(row[0],row[1]))

    return

@plugin_function("logs.setErrorLogVersbosity")
def set_error_log_verbosity(value=3, persist=False, session=None):
    """
    Set the values of Error Log Verbosity.

    Args:
        value (integer): The optional value of the verbosity (1=ERROR, 2=ERROR, WARNING, 3=ERROR, WARNING, INFORMATION).
                         The default is to set it to 3.
        persist (bool): Persist the change. Default: false.
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.
    """
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return
    if value not in [1, 2, 3]:
       print("ERROR: the Error Log Verbosity value must be 1, 2 or 3 !")
       return

    if persist:
        stmt = """SET PERSIST log_error_verbosity={}""".format(value)
    else:
        stmt = """SET GLOBAL log_error_verbosity={}""".format(value)
    print("Error Log Verbosity is now set to {}".format(value))
    session.run_sql(stmt)

    return

@plugin_function("logs.showErrorLog")
def show_error_log(limit=10, type="all", subsystem="all", format="table", session=None):
    """
    Display the Errog Log lines.

    Args:
        limit (integer): The amount of lines to display. 0 means no limit. Default: 10.
        type (string): The type of error entries. Valid values are 'all', 'system', 'error', 'warning' and 'note'.
                       Default is 'all'.
        subsystem (string): Filter the entries to only get this specific subsystem. Default is 'all'.
        format (string): One of table, tabbed, vertical, json, ndjson, json/raw,
              json/array, json/pretty or flat.
              Flat is like an error log with colors.
              Default is table.
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.
    """
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return
    if limit < 0 :
        print("ERROR: the limit should be a postive value !")
        return
    if limit == 0:
        limit_str = ""
    else:
        limit_str = "LIMIT {}".format(limit)
    if type.lower() not in ['all', 'system', 'error', 'warning', 'note']:
        print("ERROR: '{}' is not a valid type ! It should be 'all', 'system', 'error', 'warning' or 'note'.".format(type.lower()))
        return

    if type.lower() == 'all':
        type_str = ''
    else:
        type_str = 'PRIO = "{}" '.format(type.lower())

    if subsystem.lower() == 'all':
        subsystem_str = ''
    else:
        if type_str == '':
            subsystem_str = 'SUBSYSTEM = "{}" '.format(subsystem.lower())
        else:
            subsystem_str = 'and SUBSYSTEM = "{}" '.format(subsystem.lower())

    if subsystem_str != '' or type_str != '':
        where_str = 'WHERE'
    else:
        where_str = ''

    stmt = """(SELECT * FROM performance_schema.error_log {} {} {}
               ORDER BY LOGGED DESC {}) ORDER BY LOGGED""".format(where_str, type_str, subsystem_str, limit_str)

    if format != 'flat':
        run_and_show(stmt, format, session)
    else:
        class fg:
            error='\033[31m'
            note='\033[32m'
            warning='\033[33m'
            system='\033[34m'

        result = session.run_sql(stmt)
        rows = result.fetch_all()
        for row in rows:
            color = '\033[0m'
            if row[2] == 'System':
                color = fg.system
            if row[2] == 'Error':
                color = fg.error
            if row[2] == 'Warning':
                color = fg.warning
            if row[2] == 'Note':
                color = fg.note
            print("{}{} {} [{}] [{}] [{}] {}".format(color,row[0], row[1], row[2], row[3], row[4], row[5]))

    return


@plugin_function("logs.tailErrorLog")
def tail_error_log(limit=10, type="all", subsystem="all", refresh=1, session=None):
    """
    Tail the Errog Log lines.

    Args:
        limit (integer): The amount of lines to display when starting the tail. 0 means no limit. Default: 10.
        type (string): The type of error entries. Valid values are 'all', 'system', 'error', 'warning' and 'note'.
                       Default is 'all'.
        subsystem (string): Filter the entries to only get this specific subsystem. Default is 'all'.
        refresh (integer): amount of seconds to refresh the tail output. Default is 1.
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.
    """
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return
    if refresh < 1 :
        print("ERROR: the limit should be a postive value bigger than 0 !")
        return
    if limit < 0 :
        print("ERROR: the limit should be a postive value !")
        return
    if limit == 0:
        limit_str = ""
    else:
        limit_str = "LIMIT {}".format(limit)
    if type.lower() not in ['all', 'system', 'error', 'warning', 'note']:
        print("ERROR: '{}' is not a valid type ! It should be 'all', 'system', 'error', 'warning' or 'note'.".format(type.lower()))
        return

    if type.lower() == 'all':
        type_str = ''
    else:
        type_str = 'PRIO = "{}" '.format(type.lower())

    if subsystem.lower() == 'all':
        subsystem_str = ''
    else:
        if type_str == '':
            subsystem_str = 'SUBSYSTEM = "{}" '.format(subsystem.lower())
        else:
            subsystem_str = 'and SUBSYSTEM = "{}" '.format(subsystem.lower())

    if subsystem_str != '' or type_str != '':
        where_str = 'WHERE'
    else:
        where_str = ''


    last_date=None
    while True:
        if last_date:
            where_str = "WHERE LOGGED > '{}'".format(last_date)
            if subsystem_str != '' or type_str != '':
                where_str +=" AND"
            stmt = """(SELECT * FROM performance_schema.error_log {} {} {}
                ORDER BY LOGGED DESC) ORDER BY LOGGED""".format(where_str, type_str, subsystem_str)
        else:
            stmt = """(SELECT * FROM performance_schema.error_log {} {} {}
                ORDER BY LOGGED DESC {}) ORDER BY LOGGED""".format(where_str, type_str, subsystem_str, limit_str)

        class fg:
            error='\033[31m'
            note='\033[32m'
            warning='\033[33m'
            system='\033[34m'

        result = session.run_sql(stmt)
        rows = result.fetch_all()
        for row in rows:
            color = '\033[0m'
            if row[2] == 'System':
                color = fg.system
            if row[2] == 'Error':
                color = fg.error
            if row[2] == 'Warning':
                color = fg.warning
            if row[2] == 'Note':
                color = fg.note
            print("{}{} {} [{}] [{}] [{}] {}\033[0m".format(color,row[0], row[1], row[2], row[3], row[4], row[5]))
            last_date = row[0]
        time.sleep(refresh)

    return