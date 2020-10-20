# schema/utils.py
# -----------------
# Definition of various utilities to be used by the schema plugin
#

def __returnChecks(session, schema, table):
    # Define the query to get the routines
    stmt = """select
              concat("select '",c.table_schema,".",c.table_name,"','",c.column_name,"','",c.data_type,
                     "',","count(*) from ",c.table_schema,".",c.table_name,"
              where ",c.column_name,"  like '0000-00-00%' having count(*) > 0")
              from information_schema.columns c,information_schema.tables t
              where c.table_schema = t.table_schema and c.table_name = t.table_name
                and c.data_type in ('datetime','timestamp','date')
                and t.table_type = 'BASE TABLE'
                and t.table_schema not in ('performance_schema','information_schema','sys','mysql');"""
    if schema is not None:
       stmt  = stmt + " AND t.table_schema='%s'" % schema
    if table is not None:
       stmt  = stmt + " AND t.table_name='%s'" % table
    # Execute the query and check for warnings
    result = session.run_sql(stmt)
    defaults = result.fetch_all()
    if (result.get_warnings_count() > 0):
        # Bail out and print the warnings
        print("Warnings occurred - bailing out:")
        print(result.get_warnings())
        return False

    return defaults

def __returnDefaults(session, schema, table):
    # Define the query to get the routines
    stmt = """SELECT COLUMN_NAME ColName, COLUMN_TYPE DataType, COLUMN_DEFAULT
              FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND
              TABLE_NAME = '%s'""" % (schema, table)

    # Execute the query and check for warnings
    result = session.run_sql(stmt)
    defaults = result.fetch_all()
    if (result.get_warnings_count() > 0):
        # Bail out and print the warnings
        print("Warnings occurred - bailing out:")
        print(result.get_warnings())
        return False

    return defaults

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
