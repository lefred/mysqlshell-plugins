from mysqlsh.plugin_manager import plugin, plugin_function
from mysqlsh_plugins_common import run_and_show

@plugin
class profiling:
    """
     Statement Profiling Object using Performance Schema.

     A collection of methods to Profile MySQL Satements
    """


setup_actors = []
setup_instruments_statement = []
setup_instruments_stage = []
setup_consumers_statements = []
setup_consumers_stages = []

@plugin_function("profiling.start")
def start( session=None):
    """
    Start the profiling collection.

    This function configure Perfomance Schema Instrumentation
    to enable profiling for the current connected user.
    Using a dedicated user is recommended as all threads/connections for
    the current user will be monitored.

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

    # Check if there are other setup_actors than the default
    #
    # default is:
    # +------+------+------+---------+---------+
    # | HOST | USER | ROLE | ENABLED | HISTORY |
    # +------+------+------+---------+---------+
    # | %    | %    | %    | YES     | YES     |
    # +------+------+------+---------+---------+
    #

    stmt = """SELECT * FROM performance_schema.setup_actors"""
    result = session.run_sql(stmt)
    rows = result.fetch_all()
    #  We need to save the default
    if setup_actors == []:
        for row in rows:
            setup_actors.append({'host': row[0], 'user': row[1], 'role': row[2], 'enabled': row[3], 'history': row[4]})

    # Get the current user
    stmt = """SELECT CURRENT_USER()"""
    result = session.run_sql(stmt)
    row = result.fetch_one()
    (curr_user, curr_host) = row[0].split('@')

    # Disable instrumentation for all
    print("Disabling Actors and History for all users.")
    stmt = """UPDATE performance_schema.setup_actors
              SET ENABLED = 'NO', HISTORY = 'NO'
              WHERE HOST = '%' AND USER = '%'"""
    result = session.run_sql(stmt)
    print("Enabling Actors and History for {}@{}.".format(curr_user, curr_host))
    stmt = """INSERT INTO performance_schema.setup_actors
              (HOST,USER,ROLE,ENABLED,HISTORY)
              VALUES('{}','{}','%','YES','YES')""".format(curr_host, curr_user)
    try:
        result = session.run_sql(stmt)
    except:
        print("{}@{} is already in performance_schema.setup_actors, maybe you didn't stop profiling !".format(curr_user, curr_host))
        return

    # save the default for some tables
    stmt = """SELECT NAME, ENABLED, TIMED FROM performance_schema.setup_instruments
              WHERE NAME LIKE '%statement/%'"""
    result = session.run_sql(stmt)
    rows = result.fetch_all()
    if setup_instruments_statement == []:
        for row in rows:
            setup_instruments_statement.append({'name': row[0], 'enabled': row[1], 'timed': row[2]})

    stmt = """SELECT NAME, ENABLED, TIMED FROM performance_schema.setup_instruments
              WHERE NAME LIKE '%stage/%'"""
    result = session.run_sql(stmt)
    rows = result.fetch_all()
    if setup_instruments_stage == []:
        for row in rows:
            setup_instruments_stage.append({'name': row[0], 'enabled': row[1], 'timed': row[2]})

    stmt = """SELECT NAME, ENABLED FROM performance_schema.setup_consumers
              WHERE NAME LIKE '%events_statements_%'"""
    result = session.run_sql(stmt)
    rows = result.fetch_all()
    if setup_consumers_statements == []:
        for row in rows:
            setup_consumers_statements.append({'name': row[0], 'enabled': row[1]})

    stmt = """SELECT NAME, ENABLED FROM performance_schema.setup_consumers
              WHERE NAME LIKE '%events_stages_%'"""
    result = session.run_sql(stmt)
    rows = result.fetch_all()
    if setup_consumers_stages == []:
        for row in rows:
            setup_consumers_stages.append({'name': row[0], 'enabled': row[1]})


    stmt = """UPDATE performance_schema.setup_instruments
               SET ENABLED = 'YES', TIMED = 'YES'
               WHERE NAME LIKE '%statement/%'"""
    result = session.run_sql(stmt)

    stmt = """UPDATE performance_schema.setup_instruments
              SET ENABLED = 'YES', TIMED = 'YES'
              WHERE NAME LIKE '%stage/%'"""
    result = session.run_sql(stmt)

    stmt = """UPDATE performance_schema.setup_consumers
              SET ENABLED = 'YES'
              WHERE NAME LIKE '%events_statements_%'"""
    result = session.run_sql(stmt)

    stmt = """UPDATE performance_schema.setup_consumers
              SET ENABLED = 'YES'
              WHERE NAME LIKE '%events_stages_%'"""
    result = session.run_sql(stmt)

    print("Profiling configured for {}@{}... please use profiling.stop() to stop it".format(curr_user, curr_host))
    return

@plugin_function("profiling.stop")
def stop( session=None):
    """
    Stop the profiling collection.

    This function configure back Perfomance Schema Instrumentation
    to disable profiling for the current connected user.

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

    if setup_actors == []:
        print("Profiling was not enabled !")
        return


    stmt = """TRUNCATE TABLE performance_schema.setup_actors"""
    session.run_sql(stmt)

    for actor in setup_actors:
        stmt  = """INSERT INTO performance_schema.setup_actors
                   (HOST,USER,ROLE,ENABLED,HISTORY)
                   VALUES('{}','{}','%','YES','YES');""".format(actor['host'], actor['user'], actor['role'],
                                                                 actor['enabled'], actor['history'])
        session.run_sql(stmt)
    setup_actors.clear()

    for instrument in setup_instruments_statement:
        stmt = """UPDATE performance_schema.setup_instruments
                  SET ENABLED = '{}', TIMED = '{}'
                  WHERE NAME LIKE '{}';""".format(instrument['enabled'], instrument['timed'], instrument['name'])
        session.run_sql(stmt)
    setup_instruments_statement.clear()

    for instrument in setup_instruments_stage:
        stmt = """UPDATE performance_schema.setup_instruments
                  SET ENABLED = '{}', TIMED = '{}'
                  WHERE NAME LIKE '{}';""".format(instrument['enabled'], instrument['timed'], instrument['name'])
        session.run_sql(stmt)
    setup_instruments_stage.clear()

    for consumer in setup_consumers_statements:
        stmt = """UPDATE performance_schema.setup_consumers
                  SET ENABLED = '{}'
                  WHERE NAME LIKE '{}';""".format(consumer['enabled'], consumer['name'])
        session.run_sql(stmt)
    setup_consumers_statements.clear()

    for consumer in setup_consumers_stages:
        stmt = """UPDATE performance_schema.setup_consumers
                  SET ENABLED = '{}'
                  WHERE NAME LIKE '{}';""".format(consumer['enabled'], consumer['name'])
        session.run_sql(stmt)
    setup_consumers_stages.clear()
    print("Profiling is now stopped.")

@plugin_function("profiling.get")
def get( session=None):
    """
    Get the profile of a statement

    This function configure back Perfomance Schema Instrumentation
    to disable profiling for the current connected user.

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

    if setup_actors == []:
        print("Profiling was not started, run profiling.start() before getting any result !")
        return
    # Get the current user
    stmt = """SELECT CURRENT_USER()"""
    result = session.run_sql(stmt)
    row = result.fetch_one()
    (curr_user, curr_host) = row[0].split('@')

    # Get the list of Statements
    stmt = """(SELECT event_id, SQL_TEXT FROM performance_schema.events_statements_history_long t1
                JOIN performance_schema.threads t2 ON t2.thread_id=t1.thread_id
               WHERE PROCESSLIST_USER='{}' AND PROCESSLIST_HOST='{}'
               ORDER BY event_id DESC LIMIT 3) ORDER BY event_id""".format(curr_user, curr_host)
    result = session.run_sql(stmt)
    rows = result.fetch_all()
    for row in rows:
        print("Profiling of:")
        print("-------------")
        print(row[1])
        stmt = """SELECT event_name AS Stage, TRUNCATE(TIMER_WAIT/1000000000000,6) AS Duration
                  FROM performance_schema.events_stages_history_long
                  WHERE NESTING_EVENT_ID={}""".format(row[0])
        run_and_show(stmt)
        print("Don't forget to stop the profiling when done.")
        return


