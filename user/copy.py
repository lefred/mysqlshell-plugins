from mysqlsh.plugin_manager import plugin, plugin_function

import re

global session_destination
global server_info_destination
global server_info_origin
global session_origin

session_destination = None
session_origin = None
server_info_destination = None
server_info_origin = None

mds_allowed_privileges = [
    "ALTER",
    "ALTER ROUTINE",
    "CREATE",
    "CREATE ROLE",
    "CREATE ROUTINE",
    "CREATE TEMPORARY TABLES",
    "CREATE USER",
    "CREATE VIEW",
    "DELETE",
    "DROP",
    "DROP ROLE",
    "EVENT",
    "EXECUTE",
    "INDEX",
    "INSERT",
    "LOCK TABLES",
    "PROCESS",
    "REFERENCES",
    "REPLICATION CLIENT",
    "REPLICATION SLAVE",
    "SELECT",
    "SHOW DATABASES",
    "SHOW VIEW",
    "TRIGGER",
    "UPDATE"
]


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
def copy_users_grants(dryrun=False, ocimds=False, session=None):
    """
    Copy a user to another server

    Args:
        dryrun (bool): Don't run the statements but only shows them.
        ocimds (bool): Use OCI MDS compatibility mode.
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

    # Get the list of users
    stmt = """SELECT DISTINCT User, Host FROM mysql.user
              WHERE NOT( `account_locked`="Y" AND `password_expired`="Y" AND `authentication_string`="" ) {}
              ORDER BY User, Host;
         """.format(search_string)
    users =  session.run_sql(stmt).fetch_all()
    final_s = ""
    if len(users)>1:
        final_s = "s"
    print("{} user{} found!".format(len(users), final_s))
    for user in users:
        answer = shell.prompt("Do you want to copy [`{}`@`{}`] ? (y/N) ".format(user[0], user[1]), {'defaultValue': 'n'})
        if answer.lower() == 'y':
            # TODO: check if the user belongs to a role
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
                                    for allowed_priv in mds_allowed_privileges:
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
                    print("Warning: some grants may fail is the role is not created on the destination server.")


            stmt = """SHOW CREATE USER `{}`@`{}`""".format(user[0], user[1])
            create_user = session.run_sql(stmt).fetch_one()[0] + ";"
            create_user=create_user.replace("CREATE USER '{}'@'".format(user[0]),"CREATE USER IF NOT EXISTS '{}'@'".format(user[0]))
            if dryrun:
                print("-- User `{}`@`{}`".format(user[0], user[1]))
                print(create_user)
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
                if ocimds:
                    grant_stmt = grant[0]
                    on_stmt=re.sub(r"^.*( ON .*\..* TO .*$)",r"\1", grant_stmt)
                    grant_stmt_tmp = re.sub('^GRANT ','', grant_stmt)
                    grant_stmt_tmp = re.sub(' ON .*\..* TO .*$','', grant_stmt_tmp)
                    tab_grants = grant_stmt_tmp.split(', ')
                    tab_list = []
                    for priv in tab_grants:
                        for allowed_priv in mds_allowed_privileges:
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
                            print(".", end='')
                        except mysqlsh.DBError as err:
                            print("Aborting: {}".format(err))
                            return
            if not dryrun and len(grants)>0:
                print("\nUser copied successfully!")

    return
