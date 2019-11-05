def _get_default_auth_method(session=None):
    stmt = "select @@default_authentication_plugin";
    result = session.run_sql(stmt)   
    row = result.fetch_one()
    print("Default authentication method is %s" % row[0])


def get_user_auth_method(user=None, session=None):
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

