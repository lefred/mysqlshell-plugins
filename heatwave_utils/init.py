from mysqlsh.plugin_manager import plugin, plugin_function
from heatwave_utils.comm import __isHeatWaveOnline, __isHeatWavePlugin

@plugin
class heatwave_utils:
    """
    Heatwave Utils 

    A collection of utils to manage heatwavse
    """

# internal function to execute SQL in the current session and return RESULTSET
def __runAndReturn(session, sqltext=None) :
    stmt = ""
    if sqltext is not None:
        stmt = sqltext;
        stmt = stmt + ";"

    # Execute the query and check for warnings
    result = session.run_sql(stmt)
    if (result.get_warnings_count() > 0):
        # Bail out and print the warnings
        print("Warnings occurred - bailing out:")
        print(result.get_warnings())
        return False

    return result;

# internal function to Load call sys.heatwave_load(....) for single schema
def __loadSecTables(session, schema):
   
    # Define the query to get the routines
    if schema is not None:
        stmt = "call sys.heatwave_load(JSON_ARRAY('%s'), null)" % schema
        stmt = stmt + ";"


    # Execute the query and check for warnings
    result = __runAndReturn(session, stmt)

    return result;

# internal function to return array of Table name with SECONDARY_ENGINE=RAPID
def __returnSecEngineTables(session, schema):
   
    # Define the query to get the routines
    stmt = "select table_schema, table_name, create_options from information_schema.tables t where create_options like '%SECONDARY_ENGINE=%RAPID%'"

    if schema is not None:
       stmt  = stmt + " AND t.table_schema='%s'" % schema

    stmt = stmt + ";"
    # Execute the query and check for warnings
    result = session.run_sql(stmt)
    tables = result.fetch_all()
    if (result.get_warnings_count() > 0):
        # Bail out and print the warnings
        print("Warnings occurred - bailing out:")
        print(result.get_warnings())
        return False
    tablearray = []
    for row in tables:
        tablearray.append( row[1] )

    return tablearray;


# internal function to return an array of table name with SECONDARY ENGINE loaded.
def __returnSecLoadedTables(session, schema):
   
    # Define the query to get the routines

    stmt = "select t.schema_name, t.table_name from performance_schema.rpd_tables a , performance_schema.rpd_table_id t where a.id = t.id "
    if schema is not None:
       stmt  = stmt + " AND t.schema_name='%s'" % schema

    stmt = stmt + ";"
    # Execute the query and check for warnings
    result = session.run_sql(stmt)
    tables = result.fetch_all()
    if (result.get_warnings_count() > 0):
        # Bail out and print the warnings
        print("Warnings occurred - bailing out:")
        print(result.get_warnings())
        return False
    tablearray = []
    for row in tables:
        tablearray.append( row[1] )

    return tablearray;


@plugin_function("heatwave_utils.list_secondary_loaded_tables")
def list_sec_loaded_tables(schema=None, session=None):
    """
    Wizard to list secondary engine with data loaded tables 

    Args:
        schema (string): The session to be used on the operation.
        session (object): The optional session object

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
        print("No schema specified.")
        return

    if __isHeatWavePlugin(session) is False:
        print("No HeatWave Plugin")
        return

    if __isHeatWaveOnline(session) :
        db = session.get_schema(schema)
        tables = __returnSecLoadedTables(session, schema)
        return tables;

    return

@plugin_function("heatwave_utils.list_secondary_engine_tables")
def list_sec_engine_tables(schema=None, session=None):
    """
    Wizard to list tables with secondary engine

    Args:
        schema (string): The session to be used on the operation.
        session (object): The optional session object

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
        print("No schema specified.")
        return
	
    if __isHeatWavePlugin(session) is False:
        print("No HeatWave Plugin")
        return
    if __isHeatWaveOnline(session) :
        db = session.get_schema(schema)
        tables = __returnSecEngineTables(session, schema)
        return tables;
    return


@plugin_function("heatwave_utils.unload_schema")
def unload_schema(schema=None, session=None):
    """
    Wizard to unload secondary_engine for tables in schema

    Args:
        schema (string): The session to be used on the operation.
        session (object): The optional session object

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
        print("No schema specified.")
        return
	
    if __isHeatWavePlugin(session) is False:
        print("No HeatWave Plugin")
        return
    if __isHeatWaveOnline(session) :
        db = session.get_schema(schema)
        session.set_current_schema(schema)
        table = __returnSecLoadedTables(session, schema)
        for i in table:
            print('alter table ' + i + ' secondary_unload')
            result = session.run_sql('alter table ' + i + ' secondary_unload')
            shell.dump_rows(result)


    return


@plugin_function("heatwave_utils.unset_schema")
def unset_schema(schema=None, session=None):
    """
    Wizard to unset secondary_engine for tables in schema

    Args:
        schema (string): The session to be used on the operation.
        session (object): The optional session object

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
        print("No schema specified.")
        return
	
    if __isHeatWavePlugin(session) is False:
        print("No HeatWave Plugin")
        return

    db = session.get_schema(schema)
    session.set_current_schema(schema)
    table = __returnSecEngineTables(session, schema)
    for i in table:
        print('alter table ' + i + ' secondary_engine=null')
        result = session.run_sql('alter table ' + i + ' secondary_engine=null')
        shell.dump_rows(result)


    return

@plugin_function("heatwave_utils.load_schema")
def load_schema(schema=None, session=None):
    """
    Wizard to unset secondary_engine for tables in schema

    Args:
        schema (string): The session to be used on the operation.
        session (object): The optional session object

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
        print("No schema specified.")
        return

    if __isHeatWavePlugin(session) :
        db = session.get_schema(schema)
        session.set_current_schema(schema)
        result = __loadSecTables(session, schema)
        shell.dump_rows(result)

    return

@plugin_function("heatwave_utils.set_trace_on")
def set_trace_on( session=None):
    """
    Wizard to set trace on

    Args:
        session (object): The optional session object

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

    result = __runAndReturn(session, "SET SESSION optimizer_trace='enabled=on';")
    result = __runAndReturn(session, "SET optimizer_trace_offset=-2;")
    shell.dump_rows(result)

    return

@plugin_function("heatwave_utils.set_trace_off")
def set_trace_off( session=None):
    """
    Wizard to set trace off

    Args:
        session (object): The optional session object

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

    result = __runAndReturn(session, "SET SESSION optimizer_trace='enabled=off';")
    shell.dump_rows(result)

    return

from heatwave_utils import reports
