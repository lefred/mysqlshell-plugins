# schema/src.py
# -------------
# Definition of member functions for the schema extension object

def show_procedures(schema=None, session=None):
    """Lists all stored procedures of either all schemas or a given schema

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

    Returns:
        Nothing
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