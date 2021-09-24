from mysqlsh.plugin_manager import plugin, plugin_function
from mysqlsh_plugins_common import run_and_show
from mysqlsh_plugins_common import get_version
from qep import common

@plugin
class replication:
    """
   Replication utilities.

    A collection of tools and utilities to perform checks and fix
    MySQL Replication Channel

    """

@plugin_function("replication.status")
def status(extended=False, format="table", session=None):
    """
    Get the replication status information 

    This function prints the status of replication channels.

    Args:
        extended (bool): Use extended view. Default is False.
        format (string): One of table, tabbed, vertical, json, ndjson, json/raw,
              json/array, json/pretty or flat.
              Flat is like an error log with colors.
              Default is table.
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
    if extended:
        stmt = """SELECT
  conn_status.channel_name as channel_name,
  conn_status.service_state as IO_thread,
  applier_status.service_state as SQL_thread,
  conn_status.LAST_QUEUED_TRANSACTION as last_queued_transaction,
  applier_status.LAST_APPLIED_TRANSACTION as last_applied_transaction,
  if(LAST_APPLIED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP = 0, 0,
  LAST_APPLIED_TRANSACTION_END_APPLY_TIMESTAMP -
                            LAST_APPLIED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP) 'rep delay (sec)',                         
  LAST_QUEUED_TRANSACTION_START_QUEUE_TIMESTAMP -
                           LAST_QUEUED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP 'transport time',
  LAST_QUEUED_TRANSACTION_END_QUEUE_TIMESTAMP -
                           LAST_QUEUED_TRANSACTION_START_QUEUE_TIMESTAMP 'time RL',
  LAST_APPLIED_TRANSACTION_END_APPLY_TIMESTAMP -
                           LAST_APPLIED_TRANSACTION_START_APPLY_TIMESTAMP 'apply time',
  if(GTID_SUBTRACT(LAST_QUEUED_TRANSACTION, LAST_APPLIED_TRANSACTION) = "","0" ,
      abs(time_to_sec(if(time_to_sec(APPLYING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP)=0,0,
      timediff(APPLYING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP,now()))))) `lag_in_sec`
FROM
  performance_schema.replication_connection_status AS conn_status
JOIN performance_schema.replication_applier_status_by_worker AS applier_status
  ON applier_status.channel_name = conn_status.channel_name"""

    else:
        stmt = """SELECT
  conn_status.channel_name as channel_name,
  conn_status.service_state as IO_thread,
  applier_status.service_state as SQL_thread,  
    if(GTID_SUBTRACT(LAST_QUEUED_TRANSACTION, LAST_APPLIED_TRANSACTION) = "","0" ,
      abs(time_to_sec(if(time_to_sec(APPLYING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP)=0,0,
      timediff(APPLYING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP,now()))))) `lag_in_sec`                            
  FROM
  performance_schema.replication_connection_status AS conn_status
JOIN performance_schema.replication_applier_status_by_worker AS applier_status
  ON applier_status.channel_name = conn_status.channel_name
  order by 4 desc limit 1"""
    
    run_and_show(stmt, format, session)

    return 

@plugin_function("replication.error")
def status(session=None):
    """
    Get the eventual replication error

    This function prints the eventual replication channel error

    Args:
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

    # get all channels
    stmt = """select channel_name from performance_schema.replication_connection_status"""
    result = session.run_sql(stmt)
    rows = result.fetch_all()    
    if len(rows) > 0:
        got_error = False
        for row in rows:
            print("\033[47m\033[1;30m{}\033[0m".format(row[0]))
            # get connection errors
            stmt = """select channel_name, last_error_timestamp, last_error_message 
                    from performance_schema.replication_connection_status 
                    where channel_name = '{}' and last_error_message not like ''""".format(row[0])
            result_conn = session.run_sql(stmt)
            rows_conn = result_conn.fetch_all()
            if len(rows_conn) > 0:
                print(" \033[4;1;37mConnection error:\033[0m")
                got_error = True
                for row_conn in rows_conn:
                    print(" \033[1;37m{}\033[0m : \033[3;37m{}\033[0m".format(row_conn[1], row_conn[2]))
            # get applier_status_by_coordinator errors
            stmt = """select channel_name, last_error_timestamp, last_error_message 
                      from performance_schema.replication_applier_status_by_coordinator
                      where channel_name = '{}' and last_error_message not like ''""".format(row[0])
            result_conn = session.run_sql(stmt)
            rows_conn = result_conn.fetch_all()
            if len(rows_conn) > 0:
                print(" \033[4;1;37mApplier error:\033[0m")
                got_error = True
                for row_conn in rows_conn:
                    print(" \033[1;37m{}\033[0m : \033[3;37m{}\033[0m".format(row_conn[1], row_conn[2]))
            stmt = """select channel_name, last_error_timestamp, last_error_message 
                      from performance_schema.replication_applier_status_by_worker
                      where channel_name = '{}' and last_error_message not like ''""".format(row[0])
            result_conn = session.run_sql(stmt)
            rows_conn = result_conn.fetch_all()
            if len(rows_conn) > 0:
                got_error = True
                for row_conn in rows_conn:
                    print(" \033[1;37m{}\033[0m : \033[3;37m{}\033[0m".format(row_conn[1], row_conn[2]))        
        if not got_error:
            print("no error")

    else:
        print("This server is not a replica") 

    return

@plugin_function("replication.get_gtid_to_skip")
def get_gtid_to_skip(session=None):
    """
    Get the gtid that is breaking replication. 

    This function returns the GTID breaking replication

    Args:
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

    stmt = """select channel_name, replace(replace(regexp_substr(last_error_message, "transaction '.*'"), "transaction ",""), "'", "") `gtid_to_skip` 
              from performance_schema.replication_applier_status_by_coordinator"""
    run_and_show(stmt, "table", session)
    return

@plugin_function("replication.skip_error")
def get_gtid_to_skip(session=None):
    """
    Skip the current replication error.

    This function skip the GTID of the current replication error.

    Args:
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

    stmt = """select channel_name, replace(replace(regexp_substr(last_error_message, "transaction '.*'"), "transaction ",""), "'", "") `gtid_to_skip` 
              from performance_schema.replication_applier_status_by_coordinator"""

    result = session.run_sql(stmt)
    rows = result.fetch_all()    
    if len(rows) > 0:
        for row in rows:
            print("skiping {} for replication channel '\033[1;37m{}\033[0m'...".format(row[1], row[0]))
            stmt = "SET GTID_NEXT='{}'".format(row[1])
            session.run_sql(stmt)
            stmt = "START TRANSACTION"
            session.run_sql(stmt)
            stmt = "COMMIT"
            session.run_sql(stmt)
            stmt = "SET GTID_NEXT='AUTOMATIC'"
            session.run_sql(stmt)            
    else:
        print("No replication error to skip")        

    return    