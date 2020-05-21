# innodb/progress.py
# -----------------
# Definition of methods related to InnoDB alter table progress
#

def _is_consumer_enabled(session, shell):
     
    stmt = """select sys.ps_is_consumer_enabled("events_stages_current")"""
    result = session.run_sql(stmt)
    consumer = result.fetch_one()[0]
    ok = False
    if consumer == "NO":
        answer = shell.prompt("""The consumer for 'events_stages_current' is not enabled, 
do you want to enabled it now ? (y/N) """,
                              {'defaultValue':'n'})
        if answer.lower() == 'y':
            stmt = """UPDATE performance_schema.setup_consumers
                      SET ENABLED = 'YES'
                      WHERE NAME = 'events_stages_current'"""
            result = session.run_sql(stmt)
            ok = True
    else:
        ok = True
    
    return ok

def _are_instruments_enabled(session, shell):    

    stmt = """SELECT NAME, ENABLED
         FROM performance_schema.setup_instruments
        WHERE NAME LIKE 'stage/innodb/alter table%'
              AND PROPERTIES = 'progress' AND ENABLED='NO'"""
    result = session.run_sql(stmt)
    instruments = result.fetch_all()
    ok = False
    if len(instruments) > 0:
        instruments_str = ""
        for instrument in instruments:
            instruments_str += "%s, " % instrument[0]

        answer = shell.prompt("""Some instruments are not enabled: %s 
Do you want to enabled them now ? (y/N) """
                              % instruments_str, {'defaultValue':'n'})
        if answer.lower() == 'y':
            stmt = """UPDATE performance_schema.setup_instruments
                      SET ENABLED = 'YES', TIMED = 'YES'
                      WHERE NAME LIKE 'stage/innodb/alter table%'
              AND PROPERTIES = 'progress' AND ENABLED='NO'"""
            result = session.run_sql(stmt)
            ok = True
    else:
        ok = True
    
    return ok



def get_alter_progress(format='table', session=None):
    # Get hold of the global shell object
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    if not _are_instruments_enabled(session, shell):
        print("Aborting, instruments are not enabled")
        return
    if not _is_consumer_enabled(session, shell):
        print("Aborting, the consumer is not enabled")
        return

    stmt="""SELECT stmt.THREAD_ID, stmt.SQL_TEXT, stage.EVENT_NAME AS State,
                   stage.WORK_COMPLETED, stage.WORK_ESTIMATED,
                   lpad(CONCAT(ROUND(100*stage.WORK_COMPLETED/stage.WORK_ESTIMATED, 2),"%"),12," ") 
                   AS CompletedPct, lpad(format_pico_time(stmt.TIMER_WAIT), 10, " ") AS StartedAgo,
                   current_allocated Memory           
            FROM performance_schema.events_statements_current stmt                
            INNER JOIN sys.memory_by_thread_by_current_bytes mt  
                    ON mt.thread_id = stmt.thread_id 
            INNER JOIN performance_schema.events_stages_current stage 
                    ON stage.THREAD_ID = stmt.THREAD_ID"""
    
    result = session.run_sql(stmt)
    shell.dump_rows(result, format)
    return
