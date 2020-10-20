# innodb/bufferpool.py
# -----------------
# Definition of methods related to InnoDB buffer pool usage
#

from mysqlsh.plugin_manager import plugin, plugin_function

@plugin_function("innodb.getTablesInBP")
def get_tables_in_bp(session=None):
    """
    Prints Tables in BP with some statistics.

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
    print("Processing, this can take a while (don't forget to run ANALYZE TABLE for accurate results)...")

    stmt="""select variable_name, format_bytes(variable_value/1)
              from performance_schema.global_variables
              where variable_name = 'innodb_buffer_pool_size'"""
    bp_size = session.run_sql(stmt).fetch_one()[1]
    stmt="""select variable_name, variable_value
              from performance_schema.global_variables
              where variable_name = 'innodb_buffer_pool_instances'"""
    bp_instances = session.run_sql(stmt).fetch_one()[1]

    if int(bp_instances) > 1:
        bp_instances_str = "%s instances" % bp_instances
    else:
        bp_instances_str = "%s instance" % bp_instances

    print("InnoDB Buffer Pool Size = %s (%s)" % (bp_size, bp_instances_str))

    stmt="""SELECT t1.TABLE_NAME 'Table Name', COUNT(*) AS Pages,
                   format_bytes(SUM(IF(COMPRESSED_SIZE = 0, 16384, COMPRESSED_SIZE)))
                      AS 'Total Data in BP',
                   format_bytes(any_value(data_length)+any_value(index_length))
                      'Total Table Size',
                   lpad(concat(round(SUM(IF(COMPRESSED_SIZE = 0, 16384, COMPRESSED_SIZE))
                     /(any_value(data_length)+any_value(index_length)) * 100,2),'%'),"6"," ")
                       as 'in BP'
                   FROM INFORMATION_SCHEMA.INNODB_BUFFER_PAGE t1
                   JOIN INFORMATION_SCHEMA.TABLES t2
                     ON concat('`',t2.TABLE_SCHEMA,'`.`',t2.TABLE_NAME,'`') = t1.TABLE_NAME
                   WHERE t2.TABLE_SCHEMA  NOT IN ('mysql', 'sys')
                   GROUP BY t1.TABLE_NAME
                   ORDER BY SUM(IF(COMPRESSED_SIZE = 0, 16384, COMPRESSED_SIZE)) desc,
                                (any_value(data_length)+any_value(index_length)) desc"""
    result = session.run_sql(stmt)
    shell.dump_rows(result)
    return
