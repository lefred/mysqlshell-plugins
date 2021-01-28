from mysqlsh.plugin_manager import plugin, plugin_function
from schema import utils

@plugin_function("schema_utils.showRoutines")
def show_routines(schema=None, session=None):
    """
    Show Routines.

    Args:
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
    if schema is None:
        if session.current_schema is None:
            schema = session.current_schema.name

    procedures = utils.__returnRoutines(session, schema)

    fmt = "| {0:20s} | {1:20s} | {2:10s} | {3:25s} |"
    header = fmt.format("Schema", "Name", "Type", "Definer")
    bar = "+" + "-" * 22 + "+" + "-" * 22 + "+" + "-" * 12 + "+" + "-" * 27 + "+"
    print(bar)
    print(header)
    print(bar)

    for row in procedures:
        print (fmt.format(row[0], row[1], row[2], ex_str))

    print(bar)

    return "Total: %d" % len(defaults)
