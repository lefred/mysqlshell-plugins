from mysqlsh.plugin_manager import plugin, plugin_function
from schema import utils


@plugin_function("schema_utils.showDefaults")
def show_defaults(table, schema=None, session=None):
    """
    Lists the default values of each column in a table.

    Args:
        table (string): Table name to use.
        schema (string): Schema to use.
        session (object): The session to be used on the operation.

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
    #if not session.uri.startswith('mysqlx'):
    #        print("The session object is not using X Protocol, please connect using mysqlx.")
    #        return
    if schema is None:
        if session.current_schema is None:
            print("No schema specified. Either pass a schema name or use one")
            return
        schema = session.current_schema.name

    defaults = utils.__returnDefaults(session, schema, table)

    fmt = "| {0:30s} | {1:15s} | {2:50s} | {3:25s} |"
    header = fmt.format("ColumnName", "Type", "Default", "Example")
    bar = "+" + "-" * 32 + "+" + "-" * 17 + "+" + "-" * 52 + "+" + "-" * 27 + "+"
    print (bar)
    print (header)
    print (bar)
    for row in defaults:
        col_expr = row[2]
        if col_expr is None:
            col_expr = 'NULL'
        else:
            col_expr = col_expr.replace('\\', '')
        try:
            query = session.run_sql("select {0}".format(col_expr))
        except:
            query = session.run_sql("select '{0}'".format(col_expr))
        if col_expr == 'NULL':
            ex_str = col_expr
        else:
            try:
                #example = query.execute()
                for col in query.fetch_all():
                    ex_str = str(col[0])
            except:
                ex_str = row[2]
        print (fmt.format(row[0], row[1], col_expr, ex_str))
    print (bar)

    return "Total: %d" % len(defaults)
