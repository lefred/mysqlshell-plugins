from mysqlsh.plugin_manager import plugin, plugin_function
from mysqlsh_plugins_common import run_and_show
from innodb_cluster import secondary

@plugin_function("innodb_cluster.showClusterErrorLog")
def show_cluster_error_log(limit=10, type="all", subsystem="all", session=None):
    """
    Display the Errog Log lines for all members of a cluster.

    Args:
        limit (integer): The amount of lines to display. 0 means no limit. Default: 10.
        type (string): The type of error entries. Valid values are 'all', 'system', 'error', 'warning' and 'note'.
                       Default is 'all'.
        subsystem (string): Filter the entries to only get this specific subsystem. Default is 'all'.
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

    stmt = "select * from performance_schema.replication_group_members where member_id = @@server_uuid"
    result = session.run_sql(stmt)
    rows = result.fetch_all()
    if len(rows) == 0:
        print("ERROR: this server doesn't seem to belongs to a MySQL InnoDB Cluster or Group Replication")
        return


    secondary_sessions = {}
    secondaries = secondary._get_members(session)

    if len(secondaries) == 0:
        print("No Secondary Members discovered, use logs.showErrorLog() to see the logs on this instance !")
        return

    for second_mem in secondaries:
        secondary_sessions[second_mem] = secondary._connect_to_secondary(
            shell, session, second_mem)

    stmt = """(SELECT * FROM performance_schema.error_log {} {} {}
               ORDER BY LOGGED DESC {}) ORDER BY LOGGED""".format(where_str, type_str, subsystem_str, limit_str)

    color_tab=['\033[34m', '\033[36m', '\033[94m', '\033[36m', '\033[92m', '\033[35m', '\033[91m', '\033[33m', '\033[93m']

    i = 0
    out = []
    for second_mem in secondaries:
        result = secondary_sessions[second_mem].run_sql(stmt)
        rows = result.fetch_all()
        for row in rows:
            color = color_tab[i]
            out.append("{}{} {} [{}] [{}] [{}] {}\033[0m".format(row[0], color, row[1], row[2], row[3], row[4], row[5]))
        i+=1
    out.sort()
    for entry in out:
        print(entry)
    i=0
    print("Legend:")
    print("-------")
    for second_mem in secondaries:
        print("{}{}\033[0m".format(color_tab[i], second_mem), end ="     ")
        i+=1
    return