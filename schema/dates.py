from mysqlsh.plugin_manager import plugin, plugin_function
from schema import utils

@plugin_function("schema_utils.showInvalidDates")
def show_invalid_dates(table=None, schema=None, session=None):
    """
    Show Invalid Dates

    Args:
        table (string): The table to check
        schema (string): The schema to check.
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

    queries = utils.__returnChecks(session, schema, table)

    fmt = "| {0:30s} | {1:15s} | {2:10s} | {3:15s} |"
    header = fmt.format("Schema and Table", "Column", "Type", "# Invalid")
    bar = "+" + "-" * 32 + "+" + "-" * 17 + "+" + "-" * 12 + "+" + "-" * 17 + "+"
    print (bar)
    print (header)
    print (bar)
    for query in queries:
        result = session.run_sql(query[0])
        sw = False
        rows = result.fetch_all()
        for row in rows:
           print(fmt.format(row[0],row[1],row[2],str(row[3])))
           sw = True
        if sw:
           print(bar)
    return
