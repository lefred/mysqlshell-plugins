# schema/routines.py
# -----------------
# Definition of member functions for the schema extension object to display 
# and manage routines and procedures
#
# Usage example:
# --------------


def __returnRoutines(session, schema):
    # Define the query to get the routines
    if schema is not None:
        filters = "WHERE ROUTINE_SCHEMA = '%s'" % schema
    stmt = """SELECT ROUTINE_SCHEMA AS SchemaName, ROUTINE_NAME AS RoutineName, 
              ROUTINE_TYPE AS RoutineType, DEFINE AS RoutineDefine 
              FROM INFORMATION_SCHEMA.COLUMNS"""
    if filters:
        stmt = stmt + filters

    # Execute the query and check for warnings
    result = session.run_sql(stmt)
    #routines = result.fetch_all()
    defaults = result.fetch_all()
    if (result.get_warnings_count() > 0):
        # Bail out and print the warnings
        print("Warnings occurred - bailing out:")
        print(result.get_warnings())
        return False

    return defaults

def show_routines(schema=None, session=None):
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

    procedures = __returnRoutines(session, schema)

    fmt = "| {0:20s} | {1:20s} | {2:10s} | {3:25s} |"
    header = fmt.format("Schema", "Name", "Type", "Definer")
    bar = "+" + "-" * 22 + "+" + "-" * 22 + "+" + "-" * 12 + "+" + "-" * 27 + "+"
    print bar
    print header
    print bar
    for row in procedures:
        print fmt.format(row[0], row[1], row[2], ex_str)
    print bar

    return "Total: %d" % len(defaults)

