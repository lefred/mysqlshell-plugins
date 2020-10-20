from mysqlsh.plugin_manager import plugin, plugin_function


def _get_default_auth_method(session=None):
    stmt = "select @@default_authentication_plugin"
    result = session.run_sql(stmt)
    row = result.fetch_one()
    print("Default authentication method is %s" % row[0])


@plugin_function("security.showAuthMethods")
def get_user_auth_method(user=None, session=None):
    """
    Lists all specified authentication method and the amount of users using it.

    Args:
        user (string): User to look for, it allows %%.
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

    _get_default_auth_method(session)

    if user is None:
        stmt = """select plugin as method, count(*) as users 
                 from mysql.user group by plugin order by 2 desc"""
    else:
        stmt = """select user, host, plugin as method 
                 from mysql.user where user like '%s' order by 1""" % user

    result = session.run_sql(stmt)
    shell.dump_rows(result)
