# audi/trx.py
# -----------------
# Definition of member functions for the audit extension object to display trx info
#

def _returnBinlogEvents(session, binlog):
    stmt = "SHOW BINLOG EVENTS in '%s'" % binlog
    print(stmt)
    result = session.run_sql(stmt) 
    events = result.fetch_all()
    if (result.get_warnings_count() > 0):
        # Bail out and print the warnings
        print("Warnings occurred - bailing out:")
        print(result.get_warnings())
        return False

    return events

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



def _format_bytes(size):
    # 2**10 = 1024
    power = 2**10
    for unit in ('bytes', 'kb', 'mb', 'gb'):
       if size <= power:
           return "%d %s" % (size, unit)
       size /= power

    return "%d tb" % (size,)

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
       print("%s" % _format_bytes(row[4]-row[1]))
    return 
