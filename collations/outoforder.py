# collations/outoforder.py
# -----------------

# Identify if there are values values in a char/varchar column is out
# of order if a collation is applied.

from mysqlsh.plugin_manager import plugin, plugin_function

@plugin_function("collations.outOfOrder")
def out_of_order(table, column, collation, schema=None, session=None):
    """
    Find values in a column that becomes out of order for a given collation.

    This function will list all the values in a column that becomes 
    out of order for a given collation.

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

    charset = collation.split("_")[0];

    query = """with pairs as (select lag(%s,1) over w as v1, %s as v2 
               from %s window w as (order by %s)), results as( 
                select v1,convert(v1 using %s) > convert(v2 using %s) collate %s 
                as new_order from pairs) 
               select count(v1) as out_of_order 
               from results where new_order=1""" % (col, col, table, col, charset, charset, collation)

    result = session.run_sql(query)
    offending = result.fetch_all()

    out_of_order = offending[0][0]
    if out_of_order > 0:
        print("There are changes in the order")
    else:
        print("The order is retained")
