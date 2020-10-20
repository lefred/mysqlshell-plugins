from mysqlsh.plugin_manager import plugin, plugin_function

@plugin_function("schema_utils.showProcedures")
def show_procedures(schema=None, session=None):
    """
    Lists all stored procedures of either all schemas or a given schema.

    This function will list the names of all stored procedures of the given
    schema using the the given session to query the database.

    If no session is given, the current session of the MySQL Shell will be used.

    If no schema is given and there is no current schema set in the current
    session, all stored procedures of all schemas will be listed. Otherwise,
    only the stored procedures of the schema will be listed.

    Args:
        schema (string): The optional name of a schema to be used.
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    """

    # Get hold of the global shell object
    import mysqlsh
    shell = mysqlsh.globals.shell

    # Check if the user provided a session or there is an active global session
    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                "function or connect the shell to a database.")
            return

    # Check if the user did provided a schema name, if not, try to use the
    # current active schema of the session
    schema_name = ""
    if schema is None:
        try:
            try:
                # When using MySQL X Protocol, the session object has a
                # .get_current_schema() function
                schema_name = session.get_current_schema().name
            except:
                schema_name = session.run_sql("SELECT schema()").fetch_one()[0]
        except:
            pass
    elif isinstance(schema, str):
        schema_name = schema
    else:
        print("The schema name needs to be provided as a string.")
        return

    # If no schema name was provided and there is no current schema, show
    # procedures from all schemas
    if not schema_name:
        r = session.run_sql("SELECT ROUTINE_SCHEMA, ROUTINE_NAME "
            "FROM INFORMATION_SCHEMA.ROUTINES")
    # If a schema name was provided or there is a current schema, just show
    # the procedures of that schema
    else:
        r = session.run_sql("SELECT ROUTINE_NAME "
            "FROM INFORMATION_SCHEMA.ROUTINES "
            "WHERE ROUTINE_SCHEMA = ?", [schema_name])

    # Print the result as a result grid
    shell.dump_rows(r)

@plugin_function("schema_utils.deleteProcedures")
def delete_procedures(schema=None, routine=None, session=None):
    """
    Delete stored procedures.

    Delete all stored procedures of either all schemas or a given schema
    But mysql.* and sys.*

    This function will list the names of all stored procedures of the given
    schema using the the given session to query the database.

    If no session is given, the current session of the MySQL Shell will be used.

    If no schema is given and there is no current schema set in the current
    session, all stored procedures of all schemas will be listed. Otherwise,
    only the stored procedures of the schema will be listed.

    Args:
        schema (string): The schema which to delete the stored procedures from.
        routine (string): The routine/procedure to be deleted.
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.
    """

    # Get hold of the global shell object
    import mysqlsh
    shell = mysqlsh.globals.shell

    # Check if the user provided a session or there is an active global session
    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                "function or connect the shell to a database.")
            return

    # Check if the user did provided a schema name, if not, try to use the
    # current active schema of the session
    schema_name = ""
    if schema is None:
        try:
            try:
                # When using MySQL X Protocol, the session object has a
                # .get_current_schema() function
                schema_name = session.get_current_schema().name
            except:
                schema_name = session.run_sql("SELECT schema()").fetch_one()[0]
        except:
            pass
    elif isinstance(schema, str):
        schema_name = schema
    else:
        print("The schema name needs to be provided as a string.")
        return

    if schema_name in ["sys", "mysql"] and routine is None:
        print("All routines in the schema provided cannot be removed automatically.")
        return

    # If no schema name was provided and there is no current schema, show
    # procedures from all schemas
    if not schema_name:
        r = session.run_sql("SELECT sys.quote_identifier(ROUTINE_SCHEMA) AS RoutineSchema,"
            "sys.quote_identifier(ROUTINE_NAME) AS RoutineName, ROUTINE_TYPE "
            "FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA NOT IN ('mysql', 'sys')")
    # If a schema name was provided or there is a current schema, just show
    # the procedures of that schema
    elif routine is None:
        r = session.run_sql("SELECT sys.quote_identifier(ROUTINE_SCHEMA) AS RoutineSchema,"
            "sys.quote_identifier(ROUTINE_NAME) AS RoutineName, ROUTINE_TYPE "
            "FROM INFORMATION_SCHEMA.ROUTINES "
            "WHERE ROUTINE_SCHEMA = ?", [schema_name])
    else:
        r = session.run_sql("SELECT sys.quote_identifier(ROUTINE_SCHEMA) AS RoutineSchema,"
            "sys.quote_identifier(ROUTINE_NAME) AS RoutineName, ROUTINE_TYPE "
            "FROM INFORMATION_SCHEMA.ROUTINES "
            "WHERE ROUTINE_SCHEMA = ? AND ROUTINE_NAME LIKE ?", [schema_name, routine])

    #shell.dump_rows(r)
    routines = r.fetch_all()
    sql_fmt = "DROP {2} {0}.{1}"
    for routine in routines:
        sql = sql_fmt.format(*routine)
        #print(sql)
        drop_result = session.run_sql(sql)
        if (drop_result.get_warnings_count() > 0):
            print("Warnings occurred:")
            print(result.get_warnings())
    print ("Total dropped: %d" % len(routines))
    return
