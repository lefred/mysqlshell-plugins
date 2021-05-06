# mysqlsh_plugins_common.py
# -------------------------
# This file holds common code that is shared among the individual plugins
# located in sub-folders.

def is_consumer_enabled(event_name, session, shell):

    stmt = """SELECT NAME, ENABLED FROM performance_schema.setup_consumers
              WHERE NAME LIKE '{}' AND ENABLED='NO';""".format(event_name)
    result = session.run_sql(stmt)
    consumers = result.fetch_all()
    ok = False
    if len(consumers) > 0:
        consumers_str = ""
        for consumer in consumers:
            consumers_str += "%s, " % consumer[0]

        answer = shell.prompt("""Some consumers are not enabled: %s
Do you want to enabled them now ? (y/N) """
                              % consumers_str, {'defaultValue':'n'})
        if answer.lower() == 'y':
            stmt = """UPDATE performance_schema.setup_consumers
                      SET ENABLED = 'YES'
                      WHERE NAME LIKE '{}'
                      AND ENABLED='NO'""".format(event_name)
            result = session.run_sql(stmt)
            ok = True
    else:
        ok = True

    return ok

def are_instruments_enabled(instrument_name, session, shell):

    stmt = """SELECT NAME, ENABLED
         FROM performance_schema.setup_instruments
        WHERE NAME LIKE '{}'
              AND ENABLED='NO'""".format(instrument_name)
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
                      WHERE NAME LIKE '{}'
                      AND ENABLED='NO'""".format(instrument_name)
            result = session.run_sql(stmt)
            ok = True
    else:
        ok = True

    return ok

def get_version(session):
    result = session.run_sql("SELECT @@version")
    col = result.fetch_one()
    return col[0].split('.')[0]

def get_major_version(session):
    result = session.run_sql("SELECT @@version")
    col = result.fetch_one()
    return col[0].split('.')[0]+'.'+col[0].split('.')[1]


def run_and_show(stmt, format='table',session=None):
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    try:
        result = session.run_sql(stmt)
        shell.dump_rows(result, format)
    except:
        print("This query cannot be executed.")
    return
