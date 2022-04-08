from mysqlsh.plugin_manager import plugin, plugin_function
from mysqlsh_plugins_common import get_major_version
from user import mds

import re


def _get_server_info(session):
    return session.run_sql("""SELECT CONCAT(@@hostname," (",@@version,")")""").fetch_one()[0]


@plugin_function("user.clone")
def copy_users_grants(userfrom=None, userto=None, dryrun=False, ocimds=False, force=False, session=None):
    """
    Clone a user to the same server

    Args:
        userfrom (string): User to clone from as 'user@host'
        userto (string): User to clone to as 'user@host'
        dryrun (bool): Don't run the statements but only shows them.
        ocimds (bool): Use OCI MDS compatibility mode. Default is False.
        force (bool): Reply "yes" to all questions when the plan is to copy non iteractively. Default is False.
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
    if not userfrom:
        search_string = shell.prompt("Enter the user to search (you can use wildcards '%', leave blank for all): ")
        if len(search_string.strip()) > 0:
            search_string = 'AND user LIKE "{}"'.format(search_string)
    else:
        search_string = 'AND user = "{}"'.format(userfrom)
    if not userto:
        userto = shell.prompt("Enter the destination user: ")
    print("Info: locked users and users having expired password are not listed.")

    mysql_version = get_major_version(session)
    if mysql_version == "8.0" or mysql_version == "5.7":
        # Get the list of users
        stmt = """SELECT DISTINCT User, Host,
                     IF(authentication_string = "","NO", "YES") HAS_PWD
              FROM mysql.user
              WHERE NOT( `account_locked`="Y" AND `password_expired`="Y" AND `authentication_string`="" ) {}
              ORDER BY User, Host;
            """.format(search_string)
    else:
        stmt = """SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                 WHERE TABLE_SCHEMA='mysql' AND TABLE_NAME='user' AND COLUMN_NAME='password';"""
        old_format = session.run_sql(stmt).fetch_all()
        if len(old_format) > 0:
            stmt = """SELECT DISTINCT User, Host,
                     IF(password = "","NO", "YES") HAS_PWD
              FROM mysql.user
              WHERE NOT(`password_expired`="Y" AND `authentication_string`="" ) {}
              ORDER BY User, Host;
            """.format(search_string)
        else:
            stmt = """SELECT DISTINCT User, Host,
                     IF(authentication_string = "","NO", "YES") HAS_PWD
              FROM mysql.user
              WHERE NOT(`password_expired`="Y" AND `authentication_string`="" ) {}
              ORDER BY User, Host;
            """.format(search_string)
    users =  session.run_sql(stmt).fetch_all()
    final_s = ""
    if len(users)>1:
        final_s = "s"
    print("{} user{} found!".format(len(users), final_s))
    for user in users:
        if ocimds and user[2] == "NO":
            print("[`{}`@`{}`] is not compatible with OCI MDS as it has not password, ignoring it...".format(user[0], user[1]))
            continue
        if not force:
            answer = shell.prompt("Do you want to clone [`{}`@`{}`] ? (y/N) ".format(user[0], user[1]), {'defaultValue': 'n'})
        else:
            answer = "y"
        if answer.lower() == 'y':
            if mysql_version == "8.0":
                stmt = """SELECT from_user, from_host FROM mysql.role_edges WHERE to_user = ? and to_host = ?"""
                roles = session.run_sql(stmt, [user[0], user[1]]).fetch_all()
                if len(roles)>0:
                    for role in roles:
                            stmt = """SHOW GRANTS FOR `{}`@`{}`""".format(role[0], role[1])
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
                                    grant_stmt = grant_stmt.replace("TO `{}`@`{}`".format(user[0], user[1]), "TO {}".format(userto))
                                    if dryrun:
                                        print("{};".format(grant_stmt))
                                    else:
                                        try:
                                            session.run_sql(grant_stmt)
                                        except mysqlsh.DBError as err:
                                            print("Aborting: {}".format(err))
                                            return


            if mysql_version != "8.0" and mysql_version != "5.7":
               stmt = """SHOW GRANTS FOR `{}`@`{}`""".format(user[0], user[1])
               create_user = session.run_sql(stmt).fetch_one()[0] + ";"
               create_user=create_user.replace(" TO '{}'@'{}'".format(user[0], user[1]),"CREATE USER {}".format(userto))
               create_user = re.sub(r".*CREATE USER ","CREATE USER ", create_user)
            else:
               stmt = """SHOW CREATE USER `{}`@`{}`""".format(user[0], user[1])
               create_user = session.run_sql(stmt).fetch_one()[0] + ";"
               #print("-- DEBUG: {}".format(create_user))
               create_user=create_user.replace("CREATE USER `{}`@`{}`".format(user[0], user[1]),"CREATE USER IF NOT EXISTS {}".format(userto))               
               #print("-- DEBUG: {}".format(create_user))
            if dryrun:
                print("-- User `{}`@`{}`".format(user[0], user[1]))
                print(create_user)
            else:
                print("Clone USER `{}`@`{}` to  {} ".format(user[0], user[1], userto))
                try:
                    session.run_sql(create_user)
                except mysqlsh.DBError as err:
                    print("Aborting: {}".format(err))
                    print("-- DEBUG: {}".format(create_user))
                    return

            stmt = """SHOW GRANTS FOR `{}`@`{}`""".format(user[0], user[1])
            grants = session.run_sql(stmt).fetch_all()
            if not dryrun and len(grants)>0:
                print("Copying GRANTS.", end='')
            for grant in grants:
                if "IDENTIFIED BY PASSWORD" in grant[0]:
                    grant_stmt = re.sub(r" IDENTIFIED BY PASSWORD.*$","", grant[0])
                else:
                    grant_stmt = grant[0]
                if ocimds:
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
                if grant_stmt:
                    grant_stmt = grant_stmt.replace("TO `{}`@`{}`".format(user[0], user[1]), "TO {}".format(userto))
                    grant_stmt = grant_stmt.replace("TO '{}'@'{}'".format(user[0], user[1]), "TO {}".format(userto))
                    if dryrun:
                        print("{};".format(grant_stmt))
                    else:
                        try:
                            session.run_sql(grant_stmt)
                            print(".", end='')
                        except mysqlsh.DBError as err:
                            print("\nAborting: {}".format(err))
                            print("You may need to install mysql-client to save the password.")
                            return
            if not dryrun and len(grants)>0:
                print("\nUser(s) copied successfully!")

    return
