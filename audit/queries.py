
def _get_full_details(shell, session, original_query, schema):
       if session.get_current_schema() is None: 
           old_schema=None
           session.set_current_schema(schema)
       elif session.get_current_schema().get_name() != schema:
           old_schema=session.get_current_schema().get_name()
           session.set_current_schema(schema)
       answer = shell.prompt('Do you want to have EXPLAIN output? (y/N) ', {'defaultValue':'n'})
       if answer.lower() == 'y':
           stmt = """EXPLAIN %s""" % original_query
           print(stmt)
           result = session.run_sql(stmt)
           shell.dump_rows(result,'vertical')
       answer = shell.prompt('Do you want to have EXPLAIN in JSON format output? (y/N) ', {'defaultValue':'n'})
       if answer.lower() == 'y':
           stmt = """EXPLAIN FORMAT=json %s""" % original_query
           print(stmt)
           result = session.run_sql(stmt)
           shell.dump_rows(result,'vertical')
       answer = shell.prompt('Do you want to have EXPLAIN in TREE format output? (y/N) ', {'defaultValue':'n'})
       if answer.lower() == 'y':
           stmt = """EXPLAIN format=tree %s""" % original_query
           print(stmt)
           result = session.run_sql(stmt)
           shell.dump_rows(result,'vertical')
       answer = shell.prompt('Do you want to have EXPLAIN ANALYZE output? (y/N) ', {'defaultValue':'n'})
       if answer.lower() == 'y':
           stmt = """EXPLAIN ANALYZE %s""" % original_query
           print(stmt)
           result = session.run_sql(stmt)
           shell.dump_rows(result,'vertical')
       if old_schema:
           session.set_current_schema(old_schema)
       return

def get_queries_95_perc(limit=1, select=False, schema=None, session=None):

    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    filter = ""
    if select:
        filter += "AND query_sample_text like '%select%'"
    if schema is not None:
        filter += "AND schema_name = '%s'" % schema
   
    
    stmt = """SELECT schema_name, sys.format_time(total_latency) tot_lat,   
       exec_count, sys.format_time(total_latency/exec_count) latency_per_call, 
       t2.first_seen, t2.last_seen, query_sample_text 
       FROM sys.x$statements_with_runtimes_in_95th_percentile AS t1
       JOIN performance_schema.events_statements_summary_by_digest AS t2 
         ON t2.digest=t1.digest 
      WHERE schema_name NOT in ('performance_schema', 'sys') %s 
      ORDER BY (total_latency/exec_count) desc 
      LIMIT %d""" % (filter, limit)

    result = session.run_sql(stmt)
    shell.dump_rows(result,'vertical')

    if limit == 1:
       result = session.run_sql(stmt)
       row = result.fetch_one()
       if row:
          original_query = row[6]
          _get_full_details(shell, session, original_query, row[0])

def get_queries_ft_scan(limit=1, schema=None, session=None):

    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    filter = ""
    if schema is not None:
        filter += "AND schema_name = '%s'" % schema
   
    
    stmt = """SELECT schema_name, 
       sum_rows_examined, (sum_rows_examined/exec_count) avg_rows_call,
       sys.format_time(total_latency) tot_lat,   
       exec_count, sys.format_time(total_latency/exec_count) latency_per_call, 
       t2.first_seen, t2.last_seen, query_sample_text 
       FROM sys.x$statements_with_full_table_scans AS t1
       JOIN performance_schema.events_statements_summary_by_digest AS t2 
         ON t2.digest=t1.digest 
      WHERE schema_name NOT in ('performance_schema', 'sys') %s 
      ORDER BY (total_latency/exec_count) desc 
      LIMIT %d""" % (filter, limit)

    result = session.run_sql(stmt)
    shell.dump_rows(result,'vertical')

    if limit == 1:
       result = session.run_sql(stmt)
       row = result.fetch_one()
       if row:
          original_query = row[6]
          _get_full_details(shell, session, original_query)

def get_queries_temp_disk(limit=1, schema=None, session=None):

    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    filter = ""
    if schema is not None:
        filter += "AND schema_name = '%s'" % schema
   
    
    stmt = """SELECT schema_name, 
       sys.format_time(total_latency) tot_lat,   
       exec_count, sys.format_time(total_latency/exec_count) latency_per_call, 
       t2.first_seen, t2.last_seen, query_sample_text 
       FROM sys.x$statements_with_temp_tables AS t1
       JOIN performance_schema.events_statements_summary_by_digest AS t2 
         ON t2.digest=t1.digest 
      WHERE schema_name NOT in ('performance_schema', 'sys') 
        AND disk_tmp_tables=1 %s 
      ORDER BY (total_latency/exec_count) desc 
      LIMIT %d""" % (filter, limit)

    result = session.run_sql(stmt)
    shell.dump_rows(result,'vertical')

    if limit == 1:
       result = session.run_sql(stmt)
       row = result.fetch_one()
       if row:
          original_query = row[6]
          _get_full_details(shell, session, original_query)
