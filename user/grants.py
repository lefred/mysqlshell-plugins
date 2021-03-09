from mysqlsh.plugin_manager import plugin, plugin_function

@plugin_function("user.getUsersGrants")
def get_users_grants(session=None):
    """
    Prints CREATE USERS and GRANT STATEMENTS

    Args:
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    """
    # Get hold of the global shell object
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    # Get the list of users
    stmt = """SELECT DISTINCT User, Host FROM mysql.user
              WHERE NOT( `account_locked`="Y" AND `password_expired`="Y" AND `authentication_string`="" )
              ORDER BY User, Host;
         """
    users =  session.run_sql(stmt).fetch_all()

    for user in users:
        print("-- User `{}`@`{}`".format(user[0], user[1]))
        stmt = """SHOW CREATE USER `{}`@`{}`""".format(user[0], user[1])
        create_user = session.run_sql(stmt).fetch_one()[0] + ";"
        create_user=create_user.replace("CREATE USER '{}'@'".format(user[0]),"ALTER USER '{}'@'".format(user[0]))
        print("CREATE USER IF NOT EXISTS `{}`@`{}`;".format(user[0], user[1]))
        print(create_user)
        stmt = """SHOW GRANTS FOR `{}`@`{}`""".format(user[0], user[1])
        grants = session.run_sql(stmt).fetch_all()
        for grant in grants:
            print("{};".format(grant[0]))

    # Get the list of roles
    stmt = """SELECT DISTINCT user.user AS name, user.host, IF(from_user IS NULL,0, 1) AS active
              FROM mysql.user
              LEFT JOIN mysql.role_edges ON role_edges.from_user=user.user
              WHERE `account_locked`='Y'
                AND `password_expired`='Y'
                AND `authentication_string`=''
         """
    users =  session.run_sql(stmt).fetch_all()

    for user in users:
        print("-- Role `{}`@`{}`".format(user[0], user[1]))
        stmt = """SHOW CREATE USER `{}`@`{}`""".format(user[0], user[1])
        create_user = session.run_sql(stmt).fetch_one()[0] + ";"
        create_user=create_user.replace("CREATE USER '{}'@'".format(user[0]),"ALTER USER '{}'@'".format(user[0]))
        print("CREATE ROLE IF NOT EXISTS `{}`@`{}`;".format(user[0], user[1]))
        print(create_user)
        stmt = """SHOW GRANTS FOR `{}`@`{}`""".format(user[0], user[1])
        grants = session.run_sql(stmt).fetch_all()
        for grant in grants:
            print("{};".format(grant[0]))


    return
