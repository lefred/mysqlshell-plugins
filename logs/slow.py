from mysqlsh.plugin_manager import plugin, plugin_function
from mysqlsh_plugins_common import run_and_show
from importlib import util
from datetime import datetime

def _are_consumers_enabled(session, shell):

    stmt = """SELECT count(*)
                FROM performance_schema.setup_consumers
               WHERE NAME LIKE 'events_statements_histo%' and enabled='NO' """
    result = session.run_sql(stmt)
    consumer = result.fetch_one()[0]
    ok = False
    if int(consumer) > 0:
        answer = shell.prompt("""The consumers for 'events_statements_history' are not enabled,
do you want to enabled them now ? (y/N) """,
                              {'defaultValue':'n'})
        if answer.lower() == 'y':
            stmt = """UPDATE performance_schema.setup_consumers
                      SET ENABLED = 'YES'
                      WHERE NAME like 'events_statements_history%'"""
            result = session.run_sql(stmt)
            ok = True
    else:
        ok = True

    return ok

def _get_hostname(session):
    stmt = "select @@hostname"
    result = session.run_sql(stmt)
    return result.fetch_one()[0]
     

@plugin_function("logs.setupForSlowQueryLog")
def setup_for_slow_query_log(enable=True, session=None):
    """
    Enables instruments for fetching the required info to generate
    a slow query log file from Performance_Schema tables.

    Args:
        enable (bool): Enable the events history long instrumentation. (default: True)
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

    if enable == True:
      if not _are_consumers_enabled(session, shell):
         print("Required instruments are not enabled !")
         return
      print("Everything required is enabled")
    else:
      if _are_consumers_enabled(session, shell):
        stmt = """UPDATE performance_schema.setup_consumers
                      SET ENABLED = 'NO'
                      WHERE NAME like 'events_statements_history_long'"""
        result = session.run_sql(stmt)
        print("Required instruments are now disabled !")
        return
      print("events_statements_history_long was not enabled.")  

    return

@plugin_function("logs.generateSlowQueryLog")
def generate_slow_query_log(truncate=False, session=None):
    """
    Generates a slow query log file from Performance_Schema tables.

    Args:
        truncate (bool): Truncate the Performance_Schema tables after retrieving the data (default: False)
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

    if not _are_consumers_enabled(session, shell):
        print("Aborting, required instruments are not enabled !")
        return
            
    query = """select *, concat(date_sub(now(),
             INTERVAL (
                select VARIABLE_VALUE from performance_schema.global_status 
                  where variable_name='UPTIME')-TIMER_START*10e-13 second)) start_time,
             concat(timer_wait/1e+9) timer_wait_ms, concat(round(timer_wait/1e+12,6)) timer_wait_s,
             concat(round(lock_time/1e+12,6)) lock_time_s,
             format_pico_time(timer_wait) wait_human, 
             concat(round(unix_timestamp(date_sub(now(),INTERVAL (
                select VARIABLE_VALUE from performance_schema.global_status 
                 where variable_name='UPTIME')-TIMER_START*10e-13 second)))) timestamp_rnd,
             concat(unix_timestamp(date_sub(now(),INTERVAL (
                select VARIABLE_VALUE from performance_schema.global_status 
                 where variable_name='UPTIME')-TIMER_START*10e-13 second))) timestamp 
             from performance_schema.events_statements_history_long"""

    result = session.run_sql(query)
    content_log=""
    cpt = 0
    row = result.fetch_one_object()


    while(row):
        if row["SQL_TEXT"]:
            log_time = datetime.strptime(row["start_time"], "%Y-%m-%d %H:%M:%S.%f")
            content_log = content_log + "# Time: {}Z\n".format(log_time.isoformat("T"))
            content_log = content_log + "# User@Host: n/a [] @ n/a []  Id: {}\n".format(
                row["THREAD_ID"]
            )
            content_log = (
                content_log
                + "# Query_time: {}  Lock_time: {}  Rows_sent: {}  Rows_examined: {}  Rows_affected: {}\n".format(
                    row["timer_wait_s"],
                    row["lock_time_s"],
                    row["ROWS_SENT"],
                    row["ROWS_EXAMINED"],
                    row["ROWS_AFFECTED"],
                )
            )
            content_log = (
                content_log
                + "# Bytes_sent: n/a  Tmp_tables: {}  Tmp_disk_tables: {}  Tmp_table_sizes: n/a\n".format(
                    row["CREATED_TMP_TABLES"], row["CREATED_TMP_DISK_TABLES"]
                )
            )
            content_log = (
                content_log
                + "# Full_scan: {}  Full_join: {}  Tmp_table: {}  Tmp_table_on_disk: {}\n".format(
                    ["no", "yes"][int(row["SELECT_SCAN"]) > 0],
                    ["no", "yes"][int(row["SELECT_FULL_JOIN"]) > 0],
                    ["no", "yes"][int(row["CREATED_TMP_TABLES"]) > 0],
                    ["no", "yes"][int(row["CREATED_TMP_DISK_TABLES"]) > 0],
                )
            )
            content_log = (
                content_log
                + "# Merge_passes: {} Execution_engine: {}\n".format(
                    row["SORT_MERGE_PASSES"], row["EXECUTION_ENGINE"]
                )
            )
            content_log = (
                content_log
                + "# No_index_used: {}  Cpu_time: {}   Max_memory: {}\n".format(
                    ["no", "yes"][int(row["NO_INDEX_USED"]) > 0],
                    row["CPU_TIME"],
                    row["MAX_TOTAL_MEMORY"],
                )
            )
            content_log = content_log + "SET timestamp={};\n".format(
                row["timestamp_rnd"]
            )
            content_log = content_log + "{};\n".format(row["SQL_TEXT"])
        row = result.fetch_one_object()
        cpt += 1


    if (truncate == True):
        query = "truncate table performance_schema.events_statements_history_long"
        session.run_sql(query)

    slowlog_name = _get_hostname(session)
    slowlog_name = slowlog_name.replace(" ", "_")
    slowlog_name = "slow_{}_{}.log".format(
           slowlog_name, datetime.utcnow().strftime("%Y%m%d%H%M")
    )    
    file_name = shell.prompt("""Where do you want to store the slow query log ? ({})""".format(slowlog_name),
                              {'defaultValue': slowlog_name})
    if file_name == "":
        file_name = slowlog_name
    file = open(file_name, "w")
    file.write(content_log)
    file.close()
    if cpt > 1:
        s_end = "ies"
    else:
        s_end = "y"
    print("Slow query log with {} entr{} generated as {}".format(cpt, s_end, file_name))
    return
