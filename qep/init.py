from mysqlsh.plugin_manager import plugin, plugin_function
from mysqlsh_plugins_common import get_version
from qep import common

global dbok

dbok=False

@plugin
class qep:
    """
    Query Execution Plan utilities.

    A collection of tools and utilities to perform checks on your
    MySQL Query Execution Plan
    """

@plugin_function("qep.get")
def get_qep(session=None):
    """
    Prints the Query Execution Plan for a query

    This function prints the QEP for a query in different format:
       - traditional
       - tree
       - json
    It will also run EXPLAIN ANALYZE and compare with existing
    saved QEP for the query if any.
    It will also propose to save the QEP in a dedicate schema and
    table: dba.qep

    Args:
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    """
    import mysqlsh
    shell = mysqlsh.globals.shell

    global dbok

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return
    if shell.parse_uri(session.get_uri())['scheme'] != "mysqlx":
        print("For more details, please use a MySQL X connection.")
        return
    if session.get_current_schema() is None:
        print("Warning: no schema has been chosen, your query needs to specify all used scheme.")
    stmt = ""
    statement_enter = shell.prompt("Enter the query (end it with ';'): ")
    while True:
        stmt += statement_enter
        if ";" in statement_enter:
            if statement_enter.index(";") == len(statement_enter.rstrip())-1:
                break
            else:
                stmt += ' '
                statement_enter = shell.prompt("> ")
        else:
            stmt += ' '
            statement_enter = shell.prompt("> ")
    #print(stmt)
    dbok = common.get_full_detail(stmt, session, dbok)

