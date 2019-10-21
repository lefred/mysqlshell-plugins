# schema/dates.py
# -----------------
# Definition of member functions for the schema extension object to display invalid dates
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


def show_invalid_dates(table=None, schema=None, session=None):
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
