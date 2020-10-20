# maintenance/shutdown.py
# -----------------
# Definition of member functions for the maintenance extension object
from mysqlsh.plugin_manager import plugin, plugin_function

import mysqlsh
from mysqlsh import mysql

shell = mysqlsh.globals.shell


def _get_std_protocol_port(session):
    stmt = "select @@port"
    result = session.run_sql(stmt)
    port = result.fetch_one()[0]
    return port


def _connect_to_std_protocol(session):
    port = _get_std_protocol_port(session)
    uri_json = shell.parse_uri(session.get_uri())
    uri_json['port'] = port
    uri_json['scheme'] = "mysql"
    uri = shell.unparse_uri(uri_json)

    print("Using %s to connect to standard protocol..." % uri)
    session2 = mysql.get_session(uri)
    return session2


def _send_to_mysql_std(session, stmt):
    result = session.run_sql(stmt)
    return


@plugin_function("maintenance.shutdown")
def shutdown(session=None):
    """
    Stop the instance using MySQL Classic Protocol.

    This function connects to the classic MySQL Protocol to stop the server.

    Args:
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    """
    # Get hold of the global shell object
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return
    session2 = _connect_to_std_protocol(session)
    print("Stopping mysqld....")
    _send_to_mysql_std(session2, "shutdown")
    session2.close()
    session.close()
    return
