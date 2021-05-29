from mysqlsh.plugin_manager import plugin, plugin_function
from mysqlsh_plugins_common import get_major_version
import re

@plugin_function("user.getUsersGrants")
def get_users_grants(find=None, exclude=None, session=None):
    """
    Prints CREATE USERS, ROLES and GRANT STATEMENTS

    Args:
        find (string): Users to find, wildcards can also be used. If none,
            all users and roles are returned. Default: None.
        exclude (string): Users to exclude, wildcards can also be used. Default: None.
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
    search_string = ""
    exclude_string = ""
    if find:
        search_string = 'AND user LIKE "{}"'.format(find)
    if exclude:
        exclude_string = 'AND user NOT LIKE "{}"'.format(exclude)

    # Get the version
    mysql_version = get_major_version(session)
    if mysql_version == "8.0":
        # Get the list of roles
        stmt = """SELECT DISTINCT user.user AS name, user.host, IF(from_user IS NULL,0, 1) AS active
              FROM mysql.user
              LEFT JOIN mysql.role_edges ON role_edges.from_user=user.user
              WHERE `account_locked`='Y'
                AND `password_expired`='Y'
                AND `authentication_string`=''
                {} {}
            """.format(search_string, exclude_string)
        users =  session.run_sql(stmt).fetch_all()
        print("-- INFO: due to eventual binary values in the authentication string, it's better to output the result to a file.")
        for user in users:
            print("-- Role `{}`@`{}`".format(user[0], user[1]))
            stmt = """SHOW CREATE USER `{}`@`{}`""".format(user[0], user[1])
            create_user = session.run_sql(stmt).fetch_one()[0] + ";"
            create_user=create_user.replace("CREATE USER '{}'@'".format(user[0]),"CREATE USER IF NOT EXISTS '{}'@'".format(user[0]))
            #print("CREATE ROLE IF NOT EXISTS `{}`@`{}`;".format(user[0], user[1]))
            print(create_user)
            stmt = """SHOW GRANTS FOR `{}`@`{}`""".format(user[0], user[1])
            grants = session.run_sql(stmt).fetch_all()
            for grant in grants:
                print("{};".format(grant[0]))

        # Get the list of users
        stmt = """SELECT DISTINCT User, Host FROM mysql.user
              WHERE NOT( `account_locked`="Y" AND `password_expired`="Y" AND `authentication_string`="" ) {} {}
              ORDER BY User, Host;
            """.format(search_string, exclude_string)
    else:
        if mysql_version == "5.7":
           stmt = """SELECT DISTINCT User, Host FROM mysql.user
              WHERE NOT( `account_locked`="Y" AND `password_expired`="Y" AND `authentication_string`="" ) {} {}
              ORDER BY User, Host;
            """.format(search_string, exclude_string)
        else:
            stmt = """SELECT DISTINCT User, Host FROM mysql.user
              WHERE NOT(`password_expired`="Y" AND `authentication_string`="" ) {} {}
              ORDER BY User, Host;
            """.format(search_string, exclude_string)
    users =  session.run_sql(stmt).fetch_all()

    for user in users:
        print("-- User `{}`@`{}`".format(user[0], user[1]))
        if mysql_version != "8.0" and mysql_version != "5.7":
            stmt = """SHOW GRANTS FOR `{}`@`{}`""".format(user[0], user[1])
            create_user = session.run_sql(stmt).fetch_one()[0] + ";"
            create_user=create_user.replace(" TO '{}'@'".format(user[0]),"CREATE USER IF NOT EXISTS '{}'@'".format(user[0]))
            create_user = re.sub(r".*CREATE USER IF NOT","CREATE USER IT NOT", create_user)
        else:
            stmt = """SHOW CREATE USER `{}`@`{}`""".format(user[0], user[1])
            create_user = session.run_sql(stmt).fetch_one()[0] + ";"
            create_user=create_user.replace("CREATE USER '{}'@'".format(user[0]),"CREATE USER IF NOT EXISTS '{}'@'".format(user[0]))
        if mysql_version != "8.0":
            create_user=create_user.replace("BY PASSWORD","WITH 'mysql_native_password' AS")
        #print("CREATE USER IF NOT EXISTS `{}`@`{}`;".format(user[0], user[1]))
        print(create_user)
        stmt = """SHOW GRANTS FOR `{}`@`{}`""".format(user[0], user[1])
        grants = session.run_sql(stmt).fetch_all()
        for grant in grants:
            grant_to_print = grant[0]
            if mysql_version != "8.0":
               grant_to_print=grant_to_print.split("IDENTIFIED BY PASSWORD")[0]
            print("{};".format(grant_to_print))

    return
