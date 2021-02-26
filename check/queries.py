from mysqlsh.plugin_manager import plugin, plugin_function
from mysqlsh_plugins_common import run_and_show


def _get_max_text_size(session):
    stmt = """SELECT VARIABLE_VALUE FROM performance_schema.global_variables
               WHERE VARIABLE_NAME = 'performance_schema_max_sql_text_length'"""
    result = session.run_sql(stmt)
    return result.fetch_one()[0]


def _get_full_details(shell, session, original_query, schema):
       max_text_size = _get_max_text_size(session)
       current_query_size = len(original_query)
       if current_query_size >= int(int(max_text_size)*.99):
           print("\nThe returned query might be not complete, you should increase 'performance_schema_max_sql_text_length'. Actually it is set to {}".format(max_text_size))
           answer = shell.prompt('Do you want to CONTINUE (not recommended)? (y/N) ', {'defaultValue':'n'})
           if answer.lower() == 'n':
               return

       old_schema=None
       if shell.parse_uri(session.get_uri())['scheme'] != "mysqlx":
           print("\nFor more details, please use a MySQL X connection.")
           return
       if session.get_current_schema() is None:
           old_schema=None
           session.set_current_schema(schema)
       elif session.get_current_schema().get_name() != schema:
           old_schema=session.get_current_schema().get_name()
           session.set_current_schema(schema)
       answer = shell.prompt('Do you want to have EXPLAIN output? (y/N) ', {'defaultValue':'n'})
       if answer.lower() == 'y':
           stmt = """EXPLAIN %s""" % original_query
           run_and_show(stmt,'vertical')
       answer = shell.prompt('Do you want to have EXPLAIN in JSON format output? (y/N) ', {'defaultValue':'n'})
       if answer.lower() == 'y':
           stmt = """EXPLAIN FORMAT=json %s""" % original_query
           run_and_show(stmt,'vertical')
       answer = shell.prompt('Do you want to have EXPLAIN in TREE format output? (y/N) ', {'defaultValue':'n'})
       if answer.lower() == 'y':
           stmt = """EXPLAIN format=tree %s""" % original_query
           run_and_show(stmt,'vertical')
       answer = shell.prompt('Do you want to have EXPLAIN ANALYZE output? (y/N) ', {'defaultValue':'n'})
       if answer.lower() == 'y':
           stmt = """EXPLAIN ANALYZE %s""" % original_query
           run_and_show(stmt,'vertical')
       if old_schema:
           session.set_current_schema(old_schema)
       return

@plugin_function("check.getSlowerQuery")
def get_queries_95_perc(limit=1, select=False, schema=None, session=None):
    """
    Prints the slowest queries.

    This function list the slower queries. If the limit is 1 you can also see
    all the details about the query.

    Args:
        limit (integer): The amount of query to return (default: 1).
        select (bool): Returns only SELECT queries.
        schema (string): The name of the schema to use. This is optional.
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

    run_and_show(stmt,'vertical')

    if limit == 1:
       result = session.run_sql(stmt)
       row = result.fetch_one()
       if row:
          original_query = row[6]
          _get_full_details(shell, session, original_query, row[0])

@plugin_function("check.getFullTableScanQuery")
def get_queries_ft_scan(limit=1, select=False, schema=None, session=None):
    """
    Prints the queries performing full table scans"

    This function list the all the queries performing Full Table Scans. If the
    limit is 1 you can also see all the details about the query.

    Args:
        limit (integer): The amount of query to return (default: 1).
        select (bool): Returns only SELECT queries.
        schema (string): The name of the schema to use. This is optional.
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

    filter = ""
    if select:
        filter += "AND query_sample_text like '%select%'"
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

    run_and_show(stmt,'vertical')

    if limit == 1:
       result = session.run_sql(stmt)
       row = result.fetch_one()
       if row:
          original_query = row[8]
          _get_full_details(shell, session, original_query, row[0])

@plugin_function("check.getQueryTempDisk")
def get_queries_temp_disk(limit=1, schema=None, session=None):
    """
    Prints the queries using temporary tables on disk.

    This function list the all the queries using temporary tables on disk.
    If the limit is 1 you can also see all the details about the query.

    Args:
        limit (integer): The amount of query to return (default: 1).
        schema (string): The name of the schema to use. This is optional.
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

    run_and_show(stmt,'vertical')

    if limit == 1:
       result = session.run_sql(stmt)
       row = result.fetch_one()
       if row:
          original_query = row[6]
          _get_full_details(shell, session, original_query, schema)

@plugin_function("check.getQueryMostRowAffected")
def get_queries_most_rows_affected(limit=1, schema=None, session=None):
    """
    Prints the statements affecting the most rows.

    This function list the all the statements affecting most rows.

    Args:
        limit (integer): The amount of query to return (default: 1).
        schema (string): The name of the schema to use. This is optional.
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    """
    filter = ""
    if schema is not None:
        filter += "where db = '%s'" % schema


    stmt = """SELECT db, rows_affected, rows_affected_avg, query_sample_text
              FROM sys.statement_analysis  as sa
              JOIN performance_schema.events_statements_summary_by_digest as ed
                ON ed.digest=sa.digest %s
              ORDER BY rows_affected_avg DESC, rows_affected DESC LIMIT %s
           """ % (filter, limit)

    run_and_show(stmt,'vertical')

@plugin_function("check.getQueryUpdatingSamePK")
def get_queries_updating_same_pk(limit=1, schema=None, session=None):
    """
    Prints the statements updating mostly the same PK.

    This finction list all the statements updatings mostly the same PK and
    therefore having to wait more. This is used to detect hotspots.

    Args:
        limit (integer): The amount of query to return (default: 1).
        schema (string): The name of the schema to use. This is optional.
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    """
    filter = ""
    if schema is not None:
        filter += "where (current_schema = '%s' or object_schema = '%s')" % (schema, schema)


    stmt = """SELECT current_schema, rows_examined, sql_text,
                     (
                         SELECT count(*)
                         FROM performance_schema.events_statements_history_long AS t2
                         WHERE t2.digest_text=t1.digest_text
                     ) AS `count`
              FROM performance_schema.events_statements_history_long AS t1
              WHERE rows_affected > 1 %s
              ORDER BY timer_wait DESC LIMIT %s
           """ % (filter, limit)

    run_and_show(stmt,'vertical')
