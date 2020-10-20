# collations/check.py
# -----------------

# Identify values in a char/varchar column that are not unique. Useful
# to check wether a collation may be used for a column that is PRIAMRY
# KEY or have a unique constraint e.g when migarting from latin1 to
# utf8mb4.

from mysqlsh.plugin_manager import plugin, plugin_function

def _check_if_collation_exists(session, collation):
   query = """SELECT COLLATION_NAME FROM INFORMATION_SCHEMA.COLLATIONS
              where COLLATION_NAME = '%s'""" % collation
   result = session.run_sql(query)
   if len(result.fetch_all()) == 1:
       return True
   return False

@plugin_function("collations.nonUnique")
def non_unique(table, column, collation, schema=None, session=None):
    """
    Find non-unique values in a column for a given collation.

    This function will list all the non-unique values in a column
    for a given collation.

    If no session is given, the current session of the MySQL Shell will be used.

    If no schema is given and there is no current schema set in the current
    session, all stored procedures of all schemas will be listed. Otherwise,
    only the stored procedures of the schema will be listed.

    Args:
        table (string): The mandatory name of a table to be used.
        column (string): The mandatory column to check.
        collation (string): The mandatory collation to check.
        schema (string): The optional name of a schema to be used.
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

    query = "select "+col+" as problematic from (select "+col+", count("+col+") over (partition by convert("+col+" using "+charset+") collate "+collation+") as cnt from "+schema+"."+table+") as t  where cnt > 1;"

    result = session.run_sql(query)
    offending = result.fetch_all()

    print("Non-Unique values")
    print("=================")
    for row in offending:
        print(row[0])
    print("=================")
    return "Total: %d non-unique values out of %d" % (len(offending), tot)
