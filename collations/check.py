# collations/check.py
# -----------------

# Identify values in a char/varchar column that are not unique. Useful
# to check wether a collation may be used for a column that is PRIAMRY
# KEY or have a unique constraint e.g when migarting from latin1 to
# utf8mb4.

def _check_if_collation_exists(session, collation):
   query = """SELECT COLLATION_NAME FROM INFORMATION_SCHEMA.COLLATIONS 
              where COLLATION_NAME = '%s'""" % collation
   result = session.run_sql(query)
   if len(result.fetch_all()) == 1:
       return True
   return False
    

def non_unique(table, col, collation, schema=None, session=None):
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
            print("No schema specified. Either pass a schema name or use one")
            return
        schema = session.current_schema.name
    if not _check_if_collation_exists(session, collation):
        print("ERROR: %s is not a valid collation !" % collation)
        return
    query = "select count(*) from %s.%s" % (schema, table)
    result = session.run_sql(query)
    tot = result.fetch_one()[0]

    charset = collation.split("_")[0];

    query = "select "+col+" as problematic from (select "+col+", count("+col+") over (partition by convert("+col+" using "+charset+") collate "+collation+") as cnt from "+table+") as t  where cnt > 1;"

    result = session.run_sql(query)
    offending = result.fetch_all()

    print("Non-Unique values")
    print("=================")
    for row in offending:
        print(row[0])
    print("=================")
    return "Total: %d non-unique values out of %d" % (len(offending), tot)
