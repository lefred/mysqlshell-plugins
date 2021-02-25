from mysqlsh.plugin_manager import plugin, plugin_function
from mysqlsh_plugins_common import run_and_show
from pathlib import Path
from datetime import datetime
import pickle
import os

@plugin
class profiling:
    """
     Statement Profiling Object using Performance Schema.

     A collection of methods to Profile MySQL Satements
    """

threads = []
setup_actors = []
setup_instruments_statement = []
setup_instruments_stage = []
setup_consumers_statements = []
setup_consumers_stages = []

monitored_thread=None
monitored_time=None

@plugin_function("profiling.start")
def start(thread=None, session=None):
    """
    Start the profiling collection.

    This function configure Perfomance Schema Instrumentation
    to enable profiling for foreground threads.
    By default the current thread will be instrumented, but it's possible
    to pick another one too.

    Args:
        thread (integer): The optional thread (processlist_id) you want to profile. Default is the current one.
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

    if setup_actors != []:
        print("It seems that profiling has beem already enabled, maybe you didn't stop profiling !")
        return
    home = str(Path.home())
    if os.path.exists("{}/.mysqlsh/pfs.pkl".format(home)):
        print("It seems that profiling has been already enabled and you closed the session without stopping profiling !")
        print("Performance Schema values were not restored, please use profiling.stop() to restore them.")
        return


    global monitored_thread
    global monitored_time

    if thread is None:
        # Get the current thread
        stmt = """SELECT thread_id, processlist_user, processlist_host FROM performance_schema.session_variables
                JOIN performance_schema.threads
                  ON processlist_id = variable_value WHERE variable_name='pseudo_thread_id'"""
        curr_string="current "
        monitored_thread = None
    else:
        stmt = """SELECT thread_id, processlist_user, processlist_host
                    FROM performance_schema.threads WHERE processlist_id={}""".format(thread)
        monitored_thread = thread
        curr_string=""
    result = session.run_sql(stmt)
    row = result.fetch_one()
    curr_thread = row[0]

    answer = shell.prompt("""To enable profiling for the {}thread({}) [{}@{}] we need to
disable instrumentation for all other threads, do you want to continue? (y/N) """.format(curr_string,
             curr_thread, row[1], row[2]), {'defaultValue':'n'})
    if answer.lower() == 'n':
        print("Aborting...")
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

    stmt = """SELECT THREAD_ID , NAME, INSTRUMENTED, HISTORY FROM  performance_schema.threads ;"""
    result = session.run_sql(stmt)
    rows = result.fetch_all()
    #  We need to save the default
    if threads == []:
        for row in rows:
            threads.append({'thread_id': row[0], 'name': row[1], 'instrumented': row[2], 'history': row[3]})

    # Disable instrumentation for all users
    print("Disabling Instrumentation and History for all existing threads and new ones.")
    stmt = """UPDATE performance_schema.setup_actors
              SET ENABLED = 'NO', HISTORY = 'NO'
              WHERE HOST = '%' AND USER = '%'"""
    result = session.run_sql(stmt)

    # Disable instrumentation for all existing threads
    stmt = """UPDATE performance_schema.threads
              SET INSTRUMENTED = 'NO', HISTORY = 'NO'"""
    result = session.run_sql(stmt)


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

    print("Enabling Total Instrumentation fot thread {}".format(curr_thread))
    stmt = """UPDATE performance_schema.threads
              SET INSTRUMENTED = 'YES', HISTORY = 'YES' WHERE thread_id={}""".format(curr_thread)
    result = session.run_sql(stmt)
    monitored_time = datetime.now()

    print("Profiling configured for the current thread ({})... please use profiling.stop() to stop it".format(curr_thread))
    home = str(Path.home())
    file = open('{}/.mysqlsh/pfs.pkl'.format(home),'wb')
    pickle.dump(threads, file)
    pickle.dump(setup_actors, file)
    pickle.dump(setup_instruments_statement, file)
    pickle.dump(setup_instruments_stage, file)
    pickle.dump(setup_consumers_statements, file)
    pickle.dump(setup_consumers_stages, file)
    file.close()
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

    global monitored_time

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return
    home = str(Path.home())
    global setup_actors
    if setup_actors == []:
        print("Profiling was not enabled in this session!")
        if os.path.exists("{}/.mysqlsh/pfs.pkl".format(home)):
            print("Previous Performance Schema values were saved, this means the session where profiling was started never stopped!")
            print("Using those values to restore Perfromance Schmea instrumentation as previously.")
            file = open('{}/.mysqlsh/pfs.pkl'.format(home),'rb')
            global threads, setup_instruments_statement, setup_instruments_stage
            global setup_consumers_statements, setup_consumers_stages
            threads=pickle.load(file)
            setup_actors=pickle.load(file)
            setup_instruments_statement=pickle.load(file)
            setup_instruments_stage=pickle.load(file)
            setup_consumers_statements=pickle.load(file)
            setup_consumers_stages=pickle.load(file)
            file.close()
        else:
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

    for thread in threads:
        stmt = """UPDATE performance_schema.threads
                  SET INSTRUMENTED = '{}', HISTORY = '{}'
                  WHERE THREAD_ID ={};""".format(thread['instrumented'], thread['history'], thread['thread_id'])
        session.run_sql(stmt)
    threads.clear()

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
    monitored_time=None
    print("Profiling is now stopped and instrumentation settings restored.")
    home = str(Path.home())
    if os.path.exists("{}/.mysqlsh/pfs.pkl".format(home)):
       os.remove("{}/.mysqlsh/pfs.pkl".format(home))

@plugin_function("profiling.get")
def get(limit=5,session=None):
    """
    Get the profile of a statement

    This function configure back Perfomance Schema Instrumentation
    to disable profiling for the current connected user.

    Args:
        limit (integer): The amount of events showed to retrieve info. Default 5.
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
    if monitored_thread is None:
        where_str="@@pseudo_thread_id"
    else:
        where_str=monitored_thread

    # Get the list of Statements
    stmt = """SELECT event_id, SQL_TEXT, TRUNCATE(TIMER_WAIT/1000000000000,6) as Duration,
                     t1.thread_id,
                     DATE_SUB(NOW(), INTERVAL (SELECT VARIABLE_VALUE
                        FROM performance_schema.global_status
                       WHERE VARIABLE_NAME='UPTIME') - TIMER_END*10e-13 second) AS `end_time`
                FROM performance_schema.events_statements_history_long t1
                JOIN performance_schema.threads t2
                    ON t2.thread_id=t1.thread_id
                WHERE t2.processlist_id={}
                ORDER BY event_id DESC LIMIT {}""".format(where_str, limit)
    result = session.run_sql(stmt)
    rows = result.fetch_all()

    if len(rows) > 1:
        print("Last 5 events from the proccess list id: {}".format(where_str))
        print("-----------------------------------------"+"-"*len(str(where_str)))
        print("\033[33m---Events before profiling was started are in orange---\033[0m")

    tab_element={}
    for row in rows:
        if (datetime.strptime(str(row[4]), "%Y-%m-%d %H:%M:%S.%f") > monitored_time):
            print("\033[92m{}\033[0m : {}".format(row[0], row[1]))
        else:
            print("\033[33m{}\033[0m : {}".format(row[0], row[1]))

        tab_element[row[0]]={"sql": row[1], "duration": row[2], "thread": row[3]}

    answer = shell.prompt("""Which event do you want to profile ? """)
    if answer.isdigit():
        if int(answer) not in tab_element.keys():
            print("ERROR: element id {} not present in that list !".format(answer))
            return
    else:
        print("ERROR: [{}] is not the id of an event in the list !".format(answer))
        return



    print("\nProfiling of:")
    print("-------------")
    print(tab_element[int(answer)]['sql'])
    print("\033[33mduration: {}\033[0m".format(tab_element[int(answer)]['duration']))
    stmt = """SELECT event_name AS Stage, TRUNCATE(TIMER_WAIT/1000000000000,6) AS Duration
              FROM performance_schema.events_stages_history_long
              WHERE NESTING_EVENT_ID={} and THREAD_ID={}""".format(answer, tab_element[int(answer)]['thread'])

    run_and_show(stmt)
    print("Don't forget to stop the profiling when done.")
    return
