# schema/init.py
# --------------
# Initializes the schema_utils plugin.

# Contents
# --------
# This plugin will define the following functions
#  - deleteProcedures([schema][, routine][, session])
#  - showDefaults(table[, schema][, session])
#  - showInvalidDates([table][, schema][, session])
#  - showProcedures([schema][, session])
#  - showRoutines([schema][, session])

# Usage example:
# --------------
#
#  MySQL  > 2019-06-26 18:07:50 >
# JS> schema_utils.showDefaults('testing')
# No session specified. Either pass a session object to this function or connect the shell to a database
#  MySQL  > 2019-06-26 18:07:52 >
# JS> \c root@localhost
# Creating a session to 'root@localhost'
# Fetching schema names for autocompletion... Press ^C to stop.
# Your MySQL connection id is 15 (X protocol)
# Server version: 8.0.16 MySQL Community Server - GPL
# No default schema selected; type \use <schema> to set one.
# MySQL 8.0.16 > > localhost:33060+ > > 2019-06-26 18:08:01 >
# JS> schema_utils.showDefaults('testing')
# No schema specified. Either pass a schema name or use one
# MySQL 8.0.16 > > localhost:33060+ > > 2019-06-26 18:08:04 >
# JS> schema_utils.showDefaults('testing', 'big')
# +--------------------------------+-----------------+----------------------------------------------------+---------------------------+
# | ColumnName                     | Type            | Default                                            | Example                   |
# +--------------------------------+-----------------+----------------------------------------------------+---------------------------+
# | id                             | int             | None                                               | NULL                      |
# | varchar_val                    | varchar         | None                                               | NULL                      |
# +--------------------------------+-----------------+----------------------------------------------------+---------------------------+
# Total: 2
#
# MySQL 8.0.16 > > localhost:33060+ > > > test > 2019-06-26 18:11:01 >
# JS> schema_utils.showDefaults('default_test')
# +--------------------------------+-----------------+----------------------------------------------------+---------------------------+
# | ColumnName                     | Type            | Default                                            | Example                   |
# +--------------------------------+-----------------+----------------------------------------------------+---------------------------+
# | bi_col_exp                     | bigint          | (8 * 8)                                            | 64                        |
# | d_col                          | date            | curdate()                                          | 2019-06-26                |
# | d_col_exp                      | date            | (curdate() + 8)                                    | 20190634                  |
# | dt_col                         | datetime        | CURRENT_TIMESTAMP                                  | 2019-06-26 18:11:16       |
# | vc_col_exp                     | varchar         | concat(_utf8mb4\'test\',_utf8mb4\'test\')          | testtest                  |
# +--------------------------------+-----------------+----------------------------------------------------+---------------------------+
# Total: 5
#

from mysqlsh.plugin_manager import plugin, plugin_function
from ext.schema import utils

@plugin
class schema_utils:
    """
    Schema management and utilities.

    A collection of schema management tools and related
    utilities that work on schemas."
    """

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

    defaults = __returnDefaults(session, schema, table)

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
        query = session.run_sql("select {0}".format(col_expr))
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

    queries = __returnChecks(session, schema, table)

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

    procedures = __returnRoutines(session, schema)

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
