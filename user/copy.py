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


@plugin_function("user.copy")
def copy_users_grants(dryrun=False, ocimds=False, force=False, session=None):
    """
    Copy a user to another server

    Args:
        dryrun (bool): Don't run the statements but only shows them.
        ocimds (bool): Use OCI MDS compatibility mode. Default is False.
        force (bool): Reply "yes" to all questions when the plan is to copy multiple users. Default is False.
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
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return
    if session_origin != session:
        server_info_origin = _get_server_info(session)
    session_origin = session
    if session_destination is None:
        answer = shell.prompt("You need to specify a destination server (<user@>server<:port>): ")
        if not answer:
            print("Aborting, no destination server defined.")
            return
        else:
             session_destination = _connect_to_destination(shell, answer)
             if not session_destination:
                 return
             server_info_destination = _get_server_info(session_destination)
    else:
        answer = shell.prompt("The current destination server is [{}] - {}, is that OK ? (y/N) ".format(session_destination.get_uri(), server_info_destination), {'defaultValue': 'n'})
        if answer.lower() == 'n':
            answer = shell.prompt("You wanted to change destination server, please specify one (<user@>server<:port>): ")
            if not answer:
                print("Aborting, no destination server defined.")
                return
            else:
                session_destination = _connect_to_destination(shell, answer)
                if not session_destination:
                    return
                server_info_destination = _get_server_info(session_destination)
    search_string = shell.prompt("Enter the user to search (you can use wildcards '%', leave blank for all): ")
    if len(search_string.strip()) > 0:
        search_string = 'AND user LIKE "{}"'.format(search_string)
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
            answer = shell.prompt("Do you want to copy [`{}`@`{}`] ? (y/N) ".format(user[0], user[1]), {'defaultValue': 'n'})
        else:
            answer = "y"
        if answer.lower() == 'y':
            if mysql_version == "8.0":
                stmt = """SELECT from_user, from_host FROM mysql.role_edges WHERE to_user = ? and to_host = ?"""
                roles = session.run_sql(stmt, [user[0], user[1]]).fetch_all()
                if len(roles)>0:
                    final_s = " is"
                    question = "Do you want to copy that role ? (y/N) "
                    if len(roles)>1:
                        final_s = "s are"
                        question = "Do you want to copy those roles ? (y/N) "
                    print("The following role{} assigned to the user:".format(final_s))
                    for role in roles:
                        print("- `{}`@`{}`".format(role[0], role[1]))
                    if not force:
                        answer = shell.prompt(question, {'defaultValue': 'n'})
                    if answer.lower() == 'y':
                        for role in roles:
                            stmt = """SHOW CREATE USER `{}`@`{}`""".format(role[0], role[1])
                            create_user = session.run_sql(stmt).fetch_one()[0] + ";"
                            if dryrun:
                                print("-- Role `{}`@`{}`".format(role[0], role[1]))
                                print("CREATE ROLE IF NOT EXISTS `{}`@`{}`;".format(role[0], role[1]))
                                #print(create_user)
                            else:
                                print("Copying ROLE `{}`@`{}`: {} - {}--> {} - {}".format(role[0], role[1], session.get_uri(), server_info_origin, session_destination.get_uri(), server_info_destination))
                                try:
                                    session_destination.run_sql("CREATE ROLE IF NOT EXISTS ?@?;",[role[0], role[1]])
                                    #session_destination.run_sql(create_user)
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

                    else:
                        print("Warning: some grants may fail if the role is not created on the destination server.")
            back_tick = False
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
            if mysql_version != "8.0" and mysql_version != "5.7":
                if len(old_format) > 0 and not back_tick:
                    # we need to find the password
                    stmt = "SELECT password FROM mysql.user WHERE user='{}' AND host='{}'".format(user[0], user[1])
                    pwd = session.run_sql(stmt).fetch_one()[0]
                    create_user=create_user.replace("BY PASSWORD","WITH 'mysql_native_password' AS '{}'".format(pwd))
                else:
                    create_user=create_user.replace("BY PASSWORD","WITH 'mysql_native_password' AS")
            if dryrun:
                print("-- User `{}`@`{}`".format(user[0], user[1]))
            else:
                print("Copying USER `{}`@`{}`: {} - {} --> {} - {} ".format(user[0], user[1], session.get_uri(), server_info_origin, session_destination.get_uri(), server_info_destination))
                try:
                    session_destination.run_sql(create_user)
                except mysqlsh.DBError as err:
                    print("Aborting: {}".format(err))
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
                    if dryrun:
                        print("{};".format(grant_stmt))
                    else:
                        try:
                            session_destination.run_sql(grant_stmt)
                            print(".", end='')
                        except mysqlsh.DBError as err:
                            print("\nAborting: {}".format(err))
                            print("You may need to install mysql-client to save the password.")
                            return
            if not dryrun and len(grants)>0:
                print("\nUser(s) copied successfully!")

    return
