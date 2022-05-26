from mysqlsh.plugin_manager import plugin, plugin_function
from mysqlsh_plugins_common import get_major_version
from user import mds

import re

@plugin_function("user.getUsersGrants")
def get_users_grants(find=None, exclude=None, ocimds=False, session=None):
    """
    Prints CREATE USERS, ROLES and GRANT STATEMENTS

    Args:
        find (string): Users to find, wildcards can also be used. If none,
            all users and roles are returned. Default: None.
        exclude (string): Users to exclude, wildcards can also be used. Default: None.
        ocimds (bool): Use OCI MDS compatibility mode. Default is False
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    """
    # Get hold of the global shell object
    import mysqlsh
    shell = mysqlsh.globals.shell
    old_format = None

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
                if ocimds:
                    grant_stmt = grant[0]
                    on_stmt=re.sub(r"^.*( ON .*\..* TO .*$)",r"\1", grant_stmt)
                    grant_stmt_tmp = re.sub('^GRANT ','', grant_stmt)
                    grant_stmt_tmp = re.sub(' ON .*\..* TO .*$','', grant_stmt_tmp)
                    tab_grants = grant_stmt_tmp.split(', ')
                    tab_list = []
                    for priv in tab_grants:
                        for allowed_priv in mds.mds_allowed_privileges:
                            if allowed_priv == priv:
                                tab_list.append(allowed_priv)
                                break
                    if len(tab_list)>0:
                        grant_stmt="GRANT " + ', '.join(tab_list) + on_stmt
                    else:
                        grant_stmt=None
                else:
                    grant_stmt = grant[0]
                if grant_stmt:
                    print("{};".format(grant_stmt))

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
            stmt = """SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                 WHERE TABLE_SCHEMA='mysql' AND TABLE_NAME='user' AND COLUMN_NAME='password';"""
            old_format = session.run_sql(stmt).fetch_all()
            if len(old_format) > 0:
                stmt = """SELECT DISTINCT User, Host,
                     IF(password = "","NO", "YES") HAS_PWD
                FROM mysql.user
                WHERE NOT(`password_expired`="Y" AND `authentication_string`="" ) {} {}
                ORDER BY User, Host;
                """.format(search_string, exclude_string)
            else:
                stmt = """SELECT DISTINCT User, Host,
                     IF(authentication_string = "","NO", "YES") HAS_PWD
                FROM mysql.user
                WHERE NOT(`password_expired`="Y" AND `authentication_string`="" ) {} {}
                ORDER BY User, Host;
                """.format(search_string, exclude_string)
    users =  session.run_sql(stmt).fetch_all()

    back_tick = False
    for user in users:
        print("-- User `{}`@`{}`".format(user[0], user[1]))
        if mysql_version != "8.0" and mysql_version != "5.7":
            stmt = """SHOW GRANTS FOR `{}`@`{}`""".format(user[0], user[1])
            create_user = session.run_sql(stmt).fetch_one()[0] + ";"
            if "`{}`@".format(user[0]) in create_user: 
                create_user=create_user.replace(" TO `{}`@`".format(user[0]),"CREATE USER IF NOT EXISTS `{}`@`".format(user[0]))
                back_tick=True
            else:
                create_user=create_user.replace(" TO '{}'@'".format(user[0]),"CREATE USER IF NOT EXISTS '{}'@'".format(user[0]))
            create_user = re.sub(r".*CREATE USER IF NOT","CREATE USER IF NOT", create_user)
        else:
            stmt = """SHOW CREATE USER `{}`@`{}`""".format(user[0], user[1])
            create_user = session.run_sql(stmt).fetch_one()[0] + ";"
            create_user=create_user.replace("CREATE USER '{}'@'".format(user[0]),"CREATE USER IF NOT EXISTS '{}'@'".format(user[0]))
        if mysql_version != "8.0":
            if old_format:
                if len(old_format) > 0 and not back_tick:
                    # we need to find the password
                    stmt = "SELECT password FROM mysql.user WHERE user='{}' AND host='{}'".format(user[0], user[1])
                    pwd = session.run_sql(stmt).fetch_one()[0]
                    create_user=create_user.replace("BY PASSWORD","WITH 'mysql_native_password' AS '{}'".format(pwd))
                else:
                    create_user=create_user.replace("BY PASSWORD","WITH 'mysql_native_password' AS")
        #print("CREATE USER IF NOT EXISTS `{}`@`{}`;".format(user[0], user[1]))
        print(create_user)
        stmt = """SHOW GRANTS FOR `{}`@`{}`""".format(user[0], user[1])
        grants = session.run_sql(stmt).fetch_all()
        for grant in grants:
            grant_to_print = grant[0]
            if old_format:
                if len(old_format) > 0:
                    if "IDENTIFIED BY PASSWORD" in grant[0]:
                        grant_to_print = re.sub(r" IDENTIFIED BY PASSWORD.*$","", grant[0])
            if ocimds:
                on_stmt=re.sub(r"^.*( ON .*\..* TO .*$)",r"\1", grant_to_print)
                grant_stmt_tmp = re.sub('^GRANT ','', grant_to_print)
                grant_stmt_tmp = re.sub(' ON .*\..* TO .*$','', grant_stmt_tmp)
                tab_grants = grant_stmt_tmp.split(', ')
                tab_list = []
                for priv in tab_grants:
                    for allowed_priv in mds.mds_allowed_privileges:
                        if allowed_priv == priv:
                            tab_list.append(allowed_priv)
                            break
                if len(tab_list)>0:
                    grant_stmt="GRANT " + ', '.join(tab_list) + on_stmt
                else:
                    grant_stmt=None
                if grant_stmt:
                    print("{};".format(grant_stmt))
            else:
                print("{};".format(grant_to_print))

    return
