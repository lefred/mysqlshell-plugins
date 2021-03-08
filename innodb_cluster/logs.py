from mysqlsh.plugin_manager import plugin, plugin_function
from mysqlsh_plugins_common import run_and_show
from innodb_cluster import secondary
import time

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
            out.append("{}{} {:>4} {:9s} [{}] {:8s} {}\033[0m".format(row[0], color, row[1], "["+row[2]+"]", row[3], "["+row[4]+"]", row[5]))
        i+=1
    out.sort()
    for entry in out:
        print(entry)
    i=0
    print()
    print("Legend:")
    print("-------")
    for second_mem in secondaries:
        print("{}{}\033[0m".format(color_tab[i], second_mem), end ="     ")
        i+=1
    return

@plugin_function("innodb_cluster.tailClusterErrorLog")
def tail_cluster_error_log(wrap=True, limit=10, type="all", subsystem="all", refresh=1, session=None):
    """
    Display the Errog Log lines for all members of a cluster.

    Args:
        wrap (bool): Wrap lines and display them with indent. Default is True.
        limit (integer): The amount of lines to display when starting the tail. 0 means no limit. Default: 10.
        type (string): The type of error entries. Valid values are 'all', 'system', 'error', 'warning' and 'note'.
                       Default is 'all'.
        subsystem (string): Filter the entries to only get this specific subsystem. Default is 'all'.
        refresh (integer): amount of seconds to refresh the tail output. Default is 1.
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.
    """
    if wrap:
        try:
            import os
        except:
            print("Error importing module 'os', try:")
            print("mysqlsh --pym pip install --user os")
            exit

        try:
            import textwrap
        except:
            print("Error importing module 'textwrap', try:")
            print("mysqlsh --pym pip install --user textwrap")
            exit

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

    stmt = "select * from performance_schema.replication_group_members where member_id = @@server_uuid"
    result = session.run_sql(stmt)
    rows = result.fetch_all()
    if len(rows) == 0:
        print("ERROR: this server doesn't seem to belongs to a MySQL InnoDB Cluster or Group Replication")
        return


    secondary_sessions = {}
    secondaries = secondary._get_members(session)
    failures = {}

    if len(secondaries) == 0:
        print("No Secondary Members discovered, use logs.showErrorLog() to see the logs on this instance !")
        return

    for second_mem in secondaries:
        secondary_sessions[second_mem] = secondary._connect_to_secondary(
            shell, session, second_mem)
        failures[second_mem] = 0

    color_tab=['\033[34m', '\033[36m', '\033[94m', '\033[32m', '\033[92m', '\033[35m', '\033[91m', '\033[33m', '\033[93m']
    color_tab2=['\033[44m', '\033[46m', '\033[7;79;94m', '\033[42m', '\033[7;79;92m', '\033[45m', '\033[7;79;91m', '\033[43m', '\033[7;79;93m']

    i = 0
    t = 0
    print("Legend:")
    print("-------")
    for second_mem in secondaries:
        print("{}{}\033[0m".format(color_tab[i], second_mem), end ="     ")
        i+=1
    print()
    print()

    last_date=None
    trigger = True
    while trigger == True:
        try:
            if last_date:
                where_str = "WHERE LOGGED > '{}'".format(last_date)
                if subsystem_str != '' or type_str != '':
                    where_str +=" AND"
                stmt = """(SELECT * FROM performance_schema.error_log {} {} {}
                    ORDER BY LOGGED DESC) ORDER BY LOGGED""".format(where_str, type_str, subsystem_str)
            else:
                stmt = """(SELECT * FROM performance_schema.error_log {} {} {}
                    ORDER BY LOGGED DESC {}) ORDER BY LOGGED""".format(where_str, type_str, subsystem_str, limit_str)

            i=0;
            out = []
            for second_mem in secondaries:
                try:
                    if not second_mem in secondary_sessions:
                        secondary_sessions[second_mem] = secondary._connect_to_secondary(
                            shell, session, second_mem)
                    result = secondary_sessions[second_mem].run_sql(stmt)
                    rows = result.fetch_all()
                    for row in rows:
                        color = color_tab[i]
                        if row[2] == 'Error':
                            color += '\033[41m'
                        msg = row[5]
                        if wrap:
                            cols, swli = os.get_terminal_size()
                            wrapper=textwrap.TextWrapper()
                            wrapper.width=cols-65
                            wrapper.subsequent_indent = 63 * " " 
                            msg_tab = wrapper.wrap(msg)
                            msg="\n"
                            msg=msg.join(msg_tab)
                        j = 0
                        for second_members in secondaries:
                            msg = msg.replace(second_members, "\033[0m{}{}\033[0m{}".format(color_tab2[j], second_members, color))
                            j+=1

                        out.append("{}{} {:>4} {:9s} [{}] {:8s} {}\033[0m".format(row[0], color, row[1], "["+row[2]+"]", row[3], "["+row[4]+"]", msg))
                        if not last_date:
                            last_date = row[0]
                        if str(row[0]) > str(last_date):
                            last_date = row[0]
                        failures[second_mem]=0
                except:
                    if second_mem in failures:
                        failures[second_mem]+=1
                    else:
                        failures[second_mem]=0
                    out.append("{}=== ERROR connecting to {} ! ({}/3)===\033[0m".format(color_tab[i],second_mem, failures[second_mem]))
                    if failures[second_mem] == 3:
                        out.append("{}=== ERROR removing {} from output after 3 failures! ===\033[0m".format(color_tab[i],second_mem))
                        secondaries.remove(second_mem)
                        secondary_sessions.pop(second_mem)
                        failures[second_mem] = 0

                i+=1

            out.sort()
            for entry in out:
                print(entry)

            time.sleep(refresh)
        except KeyboardInterrupt:
            trigger = False
        t+=1
        if t == 10:
            t = 0
            secondaries = secondary._get_members(session)

    return
