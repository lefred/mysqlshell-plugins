from mysqlsh.plugin_manager import plugin, plugin_function

@plugin_function("innodb.getFragmentedTables")
def get_fragmented_tables(percentage=10, session=None):
    """
    Prints InnoDB fragmented tables.

    This function prints the list of InnoDB Tables having free blocks.

    Args:
        percentage (integer): The amount of free space to be considered as fragmented (default: 10).
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    """
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                "function or connect the shell to a database")
            return
    stmt = "select @@information_schema_stats_expiry;"
    result = session.run_sql(stmt)
    stats = result.fetch_all()
    if len(stats) > 0:
       for stat in stats:
           if int(stat[0]) > 0:
               print ("Warning: information_schema_stats_expiry is set to {0}.".format(*stat))
               if shell.options.interactive:
                  answer = shell.prompt("""Do you want to change it ? (y/N) """
                                 , {'defaultValue':'n'})
                  if answer.lower() == 'y':
                    stmt = """SET information_schema_stats_expiry=0"""
                    result = session.run_sql(stmt)
               else:
                    print("Changing information_schema_stats_expiry to 0 for this session only")
                    stmt = """SET information_schema_stats_expiry=0"""
                    result = session.run_sql(stmt)
    stmt = """SELECT CONCAT(table_schema, '.', table_name) as 'TABLE',
       ENGINE, CONCAT(ROUND(table_rows / 1000000, 2), 'M') `ROWS`,
       CONCAT(ROUND(data_length / ( 1024 * 1024 * 1024 ), 2), 'G') DATA,
       CONCAT(ROUND(index_length / ( 1024 * 1024 * 1024 ), 2), 'G') IDX,
       CONCAT(ROUND(( data_length + index_length ) / ( 1024 * 1024 * 1024 ), 2), 'G') 'TOTAL SIZE',
       ROUND(index_length / data_length, 2)  IDXFRAC,
        CONCAT(ROUND(( data_free / 1024 / 1024),2), 'MB') AS data_free,
       CONCAT('(',
        IF(data_free< ( data_length + index_length ),
        CONCAT(round(data_free/(data_length+index_length)*100,2),'%'),
       '100%'),')') AS data_free_pct
       FROM information_schema.TABLES  WHERE (data_free/(data_length+index_length)*100) > {limit}
       AND table_schema <> 'mysql';""".format(limit=percentage)
    result = session.run_sql(stmt)
    shell.dump_rows(result)
    print ("Don't forget to run 'ANALYZE TABLE ...' for a more accurate result.")


@plugin_function("innodb.getFragmentedTablesDisk")
def get_fragmented_tables_disk(percentage=10, session=None):
    """
    Prints InnoDB fragmented tables with disk info.

    Args:
        percentage (integer): The amount of free space to be considered as fragmented (default: 10).
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    """

    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                "function or connect the shell to a database")
            return
    stmt = "select @@information_schema_stats_expiry;"
    result = session.run_sql(stmt)
    stats = result.fetch_all()
    if len(stats) > 0:
       for stat in stats:
           if int(stat[0]) > 0:
               print ("Warning: information_schema_stats_expiry is set to {0}.".format(*stat))
               if shell.options.interactive:
                  answer = shell.prompt("""Do you want to change it ? (y/N) """
                                 , {'defaultValue':'n'})
                  if answer.lower() == 'y':
                    stmt = """SET information_schema_stats_expiry=0"""
                    result = session.run_sql(stmt)
               else:
                    print("Changing information_schema_stats_expiry to 0 for this session only")
                    stmt = """SET information_schema_stats_expiry=0"""
                    result = session.run_sql(stmt)
    stmt = """SELECT name NAME ,table_rows `ROWS`, format_bytes(data_length) DATA_SIZE,
                     format_bytes(index_length) INDEX_SIZE,
                     format_bytes(data_length+index_length) TOTAL_SIZE,
                     format_bytes(data_free) DATA_FREE,
                     format_bytes(FILE_SIZE) FILE_SIZE,
                     format_bytes(FILE_SIZE - (data_length + index_length)) WASTED_SIZE,
                     CONCAT(round(((FILE_SIZE/100 - (data_length/100 + index_length/100))/(FILE_SIZE/100))*100,2),'%') 'FREE'
              FROM information_schema.TABLES as t
              JOIN information_schema.INNODB_TABLESPACES as it
                ON it.name = concat(table_schema,"/",table_name)
             WHERE data_length > 20000
             AND ((FILE_SIZE/100 - (data_length/100 + index_length/100))/(FILE_SIZE/100))*100 > {limit}
          ORDER BY (data_length + index_length) desc""".format(limit=percentage)
    result = session.run_sql(stmt)
    shell.dump_rows(result)
    print ("Don't forget to run 'ANALYZE TABLE ...' for a more accurate result.")


