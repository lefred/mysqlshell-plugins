from mysqlsh.plugin_manager import plugin, plugin_function
from mysqlsh_plugins_common import get_major_version
from user import mds

import re

global session_destination
global server_info_destination
global server_info_origin
global session_origin

session_destination = None
session_origin = None
server_info_destination = None
server_info_origin = None


def _get_server_info(session):
    return session.run_sql("""SELECT CONCAT(@@hostname," (",@@version,")")""").fetch_one()[0]


def _connect_to_destination(shell, uri):
    try:
        session2 = shell.open_session(uri)
    except:
        print("ERROR: unable to connect {}, aborting...".format(uri))
        return None
    return session2


@plugin_function("user.clone")
def clone_user(user_from, user_to, user_from_host="%", dryrun=False, ocimds=False, session=None):
    """
    Clone a user to a new user

    Args:
        user_from (string): User to clone from as 'user@host'
        user_to (string): User to clone to as username
        user_from_host (string): Host for user to clone from
        dryrun (bool): Don't run the statements but only shows them.
        ocimds (bool): Use OCI MDS compatibility mode. Default is False.
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.
    """

    global session_destination
    global session_origin
    global server_info_destination
    global server_info_origin
    # Get hold of the global shell object
    import mysqlsh
    shell = mysqlsh.globals.shell
    old_format = None

    if session is None:
        session = shell.get_session()
        if session is None:
            print("Error: No session specified. Either pass a session object to this "
                  "function or connect the shell to a database!")
            return
    if session_origin != session:
        server_info_origin = _get_server_info(session)
    session_origin = session
    if session_destination is None:
        session_destination = session_origin

    search_string = 'AND user LIKE "{}" AND host = "{}"'.format(user_from, user_from_host)
    print("Info: locked users and users having expired password are ignored.")

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

    if len(users) > 0:
        print("Info: User '{}'@'{}' found. Continuing.".format(user_from, user_from_host))
    else:
        print("Error: User {} not found! Aborting.")
        return

    for user in users:
        if ocimds and user[2] == "NO":
            print("[`{}`@`{}`] is not compatible with OCI MDS as it has not password, ignoring it...".format(user[0], user[1]))
            continue

        if mysql_version == "8.0" and session_destination != session_origin:
            stmt = """SELECT from_user, from_host FROM mysql.role_edges WHERE to_user = ? and to_host = ?"""
            roles = session.run_sql(stmt, [user[0], user[1]]).fetch_all()
            if len(roles)>0:
                print("Info: The following role(s) are assigned to user '{}'@'{}' and would be copied:".format(user[0], user[1]))
                for role in roles:
                    print("- `{}`@`{}`".format(role[0], role[1]))
                for role in roles:
                    stmt = """SHOW CREATE USER `{}`@`{}`""".format(role[0], role[1])
                    create_user = session.run_sql(stmt).fetch_one()[0] + ";"
                    if dryrun:
                        print("CREATE ROLE IF NOT EXISTS `{}`@`{}`;".format(role[0], role[1]))
                    else:
                        print("Info: Copying role `{}`@`{}` from {} to {}".format(role[0], role[1], session.get_uri(), session_destination.get_uri()))
                        try:
                            session_destination.run_sql("CREATE ROLE IF NOT EXISTS ?@?;",[role[0], role[1]])
                        except mysqlsh.DBError as err:
                            print("Aborting: {}".format(err))
                            return
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
                            if dryrun:
                                print("{};".format(grant_stmt))
                            else:
                                try:
                                    session_destination.run_sql(grant_stmt)
                                except mysqlsh.DBError as err:
                                    print("Aborting: {}".format(err))
                                    return

        if mysql_version != "8.0" and mysql_version != "5.7":
           stmt = """SHOW GRANTS FOR `{}`@`{}`""".format(user[0], user[1])
           create_user = session.run_sql(stmt).fetch_one()[0] + ";"
           create_user=create_user.replace(" TO '{}'@'".format(user[0]),"CREATE USER IF NOT EXISTS '{}'@'".format(user_to))
           create_user = re.sub(r".*CREATE USER IF NOT","CREATE USER IF NOT", create_user)
        else:
           stmt = """SHOW CREATE USER `{}`@`{}`""".format(user[0], user[1])
           create_user = session.run_sql(stmt).fetch_one()[0] + ";"
           create_user=create_user.replace("CREATE USER '{}'@'".format(user[0]),"CREATE USER IF NOT EXISTS '{}'@'".format(user_to))
        if mysql_version != "8.0" and mysql_version != "5.7":
            if len(old_format) > 0:
                # we need to find the password
                stmt = "SELECT password FROM mysql.user WHERE user='{}' AND host='{}'".format(user[0], user[1])
                pwd = session.run_sql(stmt).fetch_one()[0]
                create_user=create_user.replace("BY PASSWORD","WITH 'mysql_native_password' AS '{}'".format(pwd))
            else:
                create_user=create_user.replace("BY PASSWORD","WITH 'mysql_native_password' AS")
        if dryrun:
            print(create_user)
        else:
            if session_destination != session_origin:
                print("Info: Cloning USER '{}'@'{}' as '{}'@'{}' from {} to {}".format(user[0], user[1], user_to, user[1], session.get_uri(), session_destination.get_uri()), end='')
            else:
                print("Info: Cloning USER '{}'@'{}' as '{}'@'{}'".format(user[0], user[1], user_to, user[1]), end='')

            try:
                session_destination.run_sql(create_user)
            except mysqlsh.DBError as err:
                print("Aborting: {}".format(err))
                return

            if not dryrun:
                print(" Done.")

        stmt = """SHOW GRANTS FOR `{}`@`{}`""".format(user[0], user[1])
        grants = session.run_sql(stmt).fetch_all()
        if not dryrun and len(grants)>0:
            print("Info: Cloning GRANTS", end='')
        for grant in grants:
            if "IDENTIFIED BY PASSWORD" in grant[0]:
                grant_stmt = re.sub(r" IDENTIFIED BY PASSWORD.*$","", grant[0])
            else:
                grant_stmt = grant[0]
            grant_stmt = grant_stmt.replace(" TO '{}'@'{}'".format(user[0], user[1]),
                                            " TO '{}'@'{}'".format(user_to, user_from_host))
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
                if dryrun:
                    print("{};".format(grant_stmt))
                else:
                    try:
                        session_destination.run_sql(grant_stmt)
                        print(".", end='')
                    except mysqlsh.DBError as err:
                        print("Aborting: {}".format(err))
                        print("You may need to install mysql-client to save the password.")
                        return
        if not dryrun:
            print(" Done.")

        if not dryrun and len(grants)>0:
            print("Info: User '{}'@'{}' cloned successfully.".format(user[0], user[1]))

    return
