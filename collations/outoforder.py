# collations/outoforder.py
# -----------------

# Identify if there are values values in a char/varchar column is out
# of order if a collation is applied.

def out_of_order(table, col, collation, schema=None, session=None):
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

    query = "with pairs as (select lag("+col+",1) over w as v1, "+col+" as v2 from "+table+" window w as (order by "+col+")), results as( select v1,convert(v1 using "+charset+") > convert(v2 using "+charset+") collate "+collation+" as new_order from pairs) select count(v1) as out_of_order from results where new_order=1";

    result = session.run_sql(query)
    offending = result.fetch_all()

    out_of_order = offending[0][0]
    if out_of_order > 0:
        print("There are changes in the order")
    else:
        print("The order is retained")
