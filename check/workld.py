from mysqlsh.plugin_manager import plugin, plugin_function
from mysqlsh_plugins_common import run_and_show


@plugin_function("check.workload")
def workload(session=None):
    """
    Prints the workload ratio between reads and writes.

    This function provides an overview of the workload. Is my application
    read or write intensive ? Use this function to get the answer.

    Args:
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

    # Get first the total server workload
    stmt = """SELECT SUM(count_read) `tot reads`,
                     CONCAT(ROUND((SUM(count_read)/SUM(count_star))*100, 2),"%") `reads`,
                     SUM(count_write) `tot writes`,
                     CONCAT(ROUND((SUM(count_write)/sum(count_star))*100, 2),"%") `writes`
              FROM performance_schema.table_io_waits_summary_by_table
              WHERE count_star > 0 ;"""
    result = session.run_sql(stmt)
    row = result.fetch_one()
    print("MySQL Workload of the server: {} reads and {} writes".format(row[1], row[3]))
    return

@plugin_function("check.workloadInfo")
def workload_info(schema=None, table=None, session=None):
    """
    Prints the workload ratio between reads and writes with some information
    per schema and tables.

    This function provides an overview of the workload. Is my application
    read or write intensive ? Use this function to get the answer.

    Args:
        schema (string): The name of the schema to check (default: None). If none is specified,
            all schemas are listed.
        table (string): The name of a specific table present in the schema defined (default: None).
            If none is specified, only the schema is listed. You can use '*' to show them all.
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

    # Get first the total server workload
    stmt = """SELECT SUM(count_read) `tot reads`,
                     CONCAT(ROUND((SUM(count_read)/SUM(count_star))*100, 2),"%") `reads`,
                     SUM(count_write) `tot writes`,
                     CONCAT(ROUND((SUM(count_write)/sum(count_star))*100, 2),"%") `writes`
              FROM performance_schema.table_io_waits_summary_by_table
              WHERE count_star > 0 ;"""
    result = session.run_sql(stmt)
    row = result.fetch_one()
    print("MySQL Workload of the server: {} reads and {} writes".format(row[1], row[3]))
    tot_reads = row[0]
    tot_writes = row[2]


    table_str = ""
    table_grp = ""
    extra = ""
    if schema is None:
        where_str = ""
    else:
        where_str = "AND object_schema='{}'".format(schema)
        if table is not None:

            stmt = """SELECT object_schema, {}
                        SUM(count_read) `tot reads`,
                        CONCAT(round((SUM(count_read)/SUM(count_star))*100, 2),"%") `reads`,
                        SUM(count_write) `tot writes`,
                        CONCAT(round((SUM(count_write)/SUM(count_star))*100, 2),"%") `writes`
                    FROM performance_schema.table_io_waits_summary_by_table
                    WHERE count_star > 0 {} GROUP BY object_schema{}""".format(table_str, where_str, table_grp)

            result = session.run_sql(stmt)
            row = result.fetch_one()
            print("      Workload for schema {}: {} reads and {} writes".format(schema, row[2], row[4]))
            schema_reads = row[1]
            schema_writes = row[3]
            extra = """CONCAT(ROUND((SUM(count_read)/{})*100, 2),"%") `ratio to schema reads`,
                       CONCAT(ROUND((SUM(count_write)/{})*100, 2),"%") `ratio to schema writes`,""".format(schema_reads, schema_writes)
            table_str = "object_name,"
            table_grp = ", object_name"

            if table == '*':
                where_str = "AND object_schema='{}'".format(schema)
            else:
                where_str = "AND object_schema='{}' AND object_name='{}'".format(schema, table)



    stmt = """SELECT object_schema, {}
                     CONCAT(ROUND((SUM(count_read)/SUM(count_star))*100, 2),"%") `reads`,
                     CONCAT(ROUND((SUM(count_write)/SUM(count_star))*100, 2),"%") `writes`,
                     {}
                     CONCAT(ROUND((SUM(count_read)/{})*100, 2),"%") `ratio to total reads`,
                     CONCAT(ROUND((SUM(count_write)/{})*100, 2),"%") `ratio to total writes`
              FROM performance_schema.table_io_waits_summary_by_table
              WHERE count_star > 0 {} GROUP BY object_schema{}""".format(table_str, extra, tot_reads, tot_writes, where_str, table_grp)
    run_and_show(stmt)

    return
