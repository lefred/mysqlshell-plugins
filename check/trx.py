# audi/trx.py
# -----------------
# Definition of member functions for the check extension object to display trx info
#

def _returnBinlogEvents(session, binlog):
    stmt = "SHOW BINLOG EVENTS in '%s'" % binlog
    result = session.run_sql(stmt) 
    events = result.fetch_all()
    if (result.get_warnings_count() > 0):
        # Bail out and print the warnings
        print("Warnings occurred - bailing out:")
        print(result.get_warnings())
        return False

    return events

def _returnBinlogName(session):
    stmt = "SHOW BINLOG EVENTS limit 1" 
    result = session.run_sql(stmt) 
    row = result.fetch_one()
    if (result.get_warnings_count() > 0):
        # Bail out and print the warnings
        print("Warnings occurred - bailing out:")
        print(result.get_warnings())
        return False

    return '.'.join(row[0].split('.')[:-1])

def _returnBinlogIO(session, name):
    stmt = """select * from sys.io_global_by_file_by_bytes 
              where file COLLATE 'utf8mb4_0900_ai_ci' 
              like '%%/%s%%' COLLATE 'utf8mb4_0900_ai_ci' order by file;""" % name
    result = session.run_sql(stmt) 

    return result

def _returnBinlogTotalIO(session):
    stmt = """select * from sys.io_global_by_wait_by_bytes 
              where event_name = 'sql/binlog'"""
    result = session.run_sql(stmt) 

    return result

def _returnBinlogs(session):
    stmt = "SHOW BINARY LOGS"
    result = session.run_sql(stmt)
    binlogs = result.fetch_all()
    if (result.get_warnings_count() > 0):
        # Bail out and print the warnings
        print("Warnings occurred - bailing out:")
        print(result.get_warnings())
        return False
    if len(binlogs) == 0:
        print("No binary log files present")
        return False

    return binlogs

def _check_for_pfs_settings(shell, session):
    # check if pfs consumers are enabled
    stmt = """select name from performance_schema.setup_consumers 
              where (name like 'events_statement%' or name like 'events_transaction%') 
              and enabled = 'NO'"""
    changes = False          
    result = session.run_sql(stmt)
    consumers = result.fetch_all()
    if len(consumers) > 0:
        consumers_str = ""
        for consumer in consumers:
            consumers_str += "%s, " % consumer[0]

        answer = shell.prompt("Some consumers (%s) are not enabled, do you want to enabled them now ? (y/N) " 
                                % consumers_str[:-2], {'defaultValue':'n'})
        if answer.lower() == 'y':
            stmt = """update performance_schema.setup_consumers 
                      set enabled = 'yes' 
                      where name like 'events_statement%' 
                      or name like 'events_transaction%'"""
            result = session.run_sql(stmt)
            changes = True
    
    # check if pfs instrument for tansaction is enabled
    stmt = """select name from performance_schema.setup_instruments 
              where name = 'transaction' and (enabled = "NO" or timed = "NO");"""
    result = session.run_sql(stmt)
    trx_instruments = result.fetch_all()
    if len(trx_instruments) > 0:
        answer = shell.prompt("The transaction consumer is not totally enabled, do you want to enabled it now ? (y/N) " 
                               , {'defaultValue':'n'})
        if answer.lower() == 'y':
            stmt = """update performance_schema.setup_instruments 
                      set enabled = 'yes', timed = 'yes' 
                      where name = 'transaction'"""
            result = session.run_sql(stmt)
            changes = True
    if changes:
        print("We just made some changes, let the system run for some time to fetch the workload")
        
    return changes


def _format_bytes(size):
    # 2**10 = 1024
    power = 2**10
    for unit in ('bytes', 'kb', 'mb', 'gb'):
       if size <= power:
           return "%d %s" % (size, unit)
       size /= power

    return "%d tb" % (size,)

def show_binlogs_io(session=None):
    # Get hold of the global shell object
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return
    binlog = _returnBinlogName(session)
    if binlog:
        rows = _returnBinlogIO(session, binlog)
        shell.dump_rows(rows)
    else:
        print("ERROR: problem getting binary log's name!")
        return False

    total_IO = _returnBinlogTotalIO(session)
    header = total_IO.get_column_names()
    rows = total_IO.fetch_all()
    for row in rows:
        print("  %s: %s     %s: %d (%s)     %s: %d (%s) " % 
                (header[2], row[2], header[6], row[6], row[7], header[9], row[9], row[10]))
    return  


def show_binlogs(session=None):
    # Get hold of the global shell object
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return
    binlogs = _returnBinlogs(session)
    if binlogs:
       print("Binary log file(s) present:")
       for entry in binlogs:
          print(entry[0]) 
 
    return  

def show_trx_size(binlog=None, session=None):
    # Get hold of the global shell object
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    binlogs = _returnBinlogs(session)
    binlog_files = []
    if binlogs:
       for entry in binlogs:
          binlog_files.append(entry[0]) 

    if binlog is None:
        binlog = binlog_files[-1]
    else:
        if binlog not in binlog_files:
           print("%s not present on the server" % binlog)
           return
    
    binlog_events = _returnBinlogEvents(session, binlog)
    print_binlog = True
    for row in binlog_events:
       if print_binlog:
          print("Transactions in binary log %s:" % row[0])
          print_binlog = False
       if row[5].startswith('BEGIN'):
           start=row[1]
       elif row[5].startswith('COMMIT'):
           print("%s" % _format_bytes(row[4]-start))
    return 

def show_trx_size_sort(limit=10,binlog=None, session=None):
    # Get hold of the global shell object
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    if not session.uri.startswith('mysqlx'):
            print("The session object is not using X Protocol, please connect using mysqlx.")
            return

    binlogs = _returnBinlogs(session)
    binlog_files = []
    if binlogs:
       for entry in binlogs:
          binlog_files.append(entry[0]) 

    if binlog is None:
        binlog = binlog_files[-1]
    else:
        if binlog not in binlog_files:
           print("%s not present on the server" % binlog)
           return
    
    binlog_events = _returnBinlogEvents(session, binlog)
    print_binlog = True
    list_binlogs=[]
    for row in binlog_events:
       if print_binlog:
          print("Transactions in binary log %s orderer by size (limit %d):" % (row[0], limit))
          print_binlog = False
       if row[5].startswith('BEGIN'):
           start=row[1]
       elif row[5].startswith('COMMIT'):
           list_binlogs.append(row[4]-start)
    list_binlogs.sort(reverse=True)
    del list_binlogs[limit:]
    for val in list_binlogs:
       print("%s" % _format_bytes(val))
    return 

def get_trx_most_stmt(limit=1, schema=None, session=None):

    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    changes = _check_for_pfs_settings(shell, session)
    if not changes:
        filter = ""
        if schema is not None:
            filter += "WHERE (object_schema = '%s' or current_schema = '%s') " % (schema, schema)
   
    
        stmt = """select t.thread_id, t.event_id, count(*) statement_count, 
                  sum(s.rows_affected) rows_affected, 
                  length(replace(group_concat(
                    case when s.event_name = "statement/sql/update" then 1 
                         when s.event_name = "statement/sql/insert" then 1 
                         when s.event_name = "statement/sql/delete" then 1 
                         else null end),',','')) 
                    as "# write statements" 
                  from performance_schema.events_transactions_history_long t 
                  join performance_schema.events_statements_history_long s 
                    on t.thread_id = s.thread_id and t.event_id = s.nesting_event_id %s
                  group by t.thread_id, t.event_id order by rows_affected desc
                  LIMIT %d""" % (filter, limit)

        result = session.run_sql(stmt)
        shell.dump_rows(result)
        result = session.run_sql(stmt)
        rows = result.fetch_all()
        if len(rows) == 1:
            answer = shell.prompt("Do you want to list all statements in that transaction ? (y/N) " 
                               , {'defaultValue':'n'})
            if answer.lower() == 'y':
                stmt = """select sql_text statements 
                          from performance_schema.events_transactions_history_long t 
                          join performance_schema.events_statements_history_long s         
                            on t.thread_id = s.thread_id 
                           and t.event_id = s.nesting_event_id where t.thread_id=%s
                          """ % rows[0][0]
                result = session.run_sql(stmt)
                stmts = result.fetch_all()
                i=1
                for stmt in stmts:
                    print(str(i) + ") " + stmt[0] + ";")
                    i += 1
        elif len(rows) > 1:
            answer = shell.prompt("Do you want to list all statements in one of these transactions ? (y/N) " 
                               , {'defaultValue':'n'})
            if answer.lower() == 'y':
                events={}
                for row in rows:
                    events[row[0]]=row[1]

                answer = shell.prompt("Which thread_id do you want to see ? (%s) " %  list(events)[0]
                               , {'defaultValue': str(list(events)[0])})
                if int(answer) in events.keys():               
                    stmt = """select sql_text statements 
                            from performance_schema.events_transactions_history_long t 
                            join performance_schema.events_statements_history_long s         
                                on t.thread_id = s.thread_id 
                            and t.event_id = s.nesting_event_id where t.thread_id=%s
                            and t.event_id = %s
                            """ % (answer , events[int(answer)])
                    result = session.run_sql(stmt)
                    stmts = result.fetch_all()
                    i=1
                    for stmt in stmts:
                        print(str(i) + ") " + stmt[0] + ";")
                        i += 1
                else:
                    print("%s is not part of the thread_id returned or is not valid!" % answer)
        
    return

def get_trx_most_rows(limit=1, schema=None, session=None):

    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    changes = _check_for_pfs_settings(shell, session)
    if not changes:
        filter = ""
        if schema is not None:
            filter += "WHERE (object_schema = '%s' or current_schema = '%s') " % (schema, schema)
   
    
        stmt = """select t.thread_id, t.event_id, count(*) statement_count, 
                    sum(s.rows_affected) rows_affected, 
                    length(replace(group_concat(
                      case 
                        when s.event_name = "statement/sql/update" then 1 
                        when s.event_name = "statement/sql/insert" then 1 
                        when s.event_name = "statement/sql/delete" then 1 
                        else null end),',','')) as "# write statements" 
                  from performance_schema.events_transactions_history_long t 
                  join performance_schema.events_statements_history_long s 
                    on t.thread_id = s.thread_id and t.event_id = s.nesting_event_id %s
                  group by t.thread_id, t.event_id order by rows_affected desc
                  LIMIT %d""" % (filter, limit)

        result = session.run_sql(stmt)
        shell.dump_rows(result)
        result = session.run_sql(stmt)
        rows = result.fetch_all()
        if len(rows) == 1:
            answer = shell.prompt("Do you want to list all statements in that transaction ? (y/N) " 
                               , {'defaultValue':'n'})
            if answer.lower() == 'y':
                stmt = """select sql_text statements 
                          from performance_schema.events_transactions_history_long t 
                          join performance_schema.events_statements_history_long s         
                            on t.thread_id = s.thread_id 
                           and t.event_id = s.nesting_event_id where t.thread_id=%s
                           and t.event_id = %s
                          """ % ( rows[0][0], rows[0][1] )
                result = session.run_sql(stmt)
                stmts = result.fetch_all()
                i=1
                for stmt in stmts:
                    print(str(i) + ") " + stmt[0] + ";")
                    i += 1
        elif len(rows) > 1:
            answer = shell.prompt("Do you want to list all statements in one of these transactions ? (y/N) " 
                               , {'defaultValue':'n'})
            if answer.lower() == 'y':
                events={}
                for row in rows:
                    events[row[0]]=row[1]

                answer = shell.prompt("Which thread_id do you want to see ? (%s) " %  list(events)[0]
                               , {'defaultValue': str(list(events)[0])})
                if int(answer) in events.keys():               
                    stmt = """select sql_text statements 
                            from performance_schema.events_transactions_history_long t 
                            join performance_schema.events_statements_history_long s         
                                on t.thread_id = s.thread_id 
                            and t.event_id = s.nesting_event_id where t.thread_id=%s
                            and t.event_id = %s
                            """ % (answer , events[int(answer)])
                    result = session.run_sql(stmt)
                    stmts = result.fetch_all()
                    i=1
                    for stmt in stmts:
                        print(str(i) + ") " + stmt[0] + ";")
                        i += 1
                else:
                    print("%s is not part of the thread_id returned or is not valid!" % answer)
        
    return
