from mysqlsh.plugin_manager import plugin, plugin_function
import mysqlsh
import time

shell = mysqlsh.globals.shell

clusterAdminPassword = None
recovery_user = None
recovery_password = None

def _check_report_host():
    result = shell.get_session().run_sql("SELECT @@report_host;").fetch_one()
    answer = shell.prompt("The server reports as [{}], is this correct ? (Y/n) ".format(result[0]),{'defaultValue': "Y"}).upper()
    if answer != 'Y':
        newname = shell.prompt("Enter the hostname that should be used as report_host: ")
        shell.get_session().run_sql("SET PERSIST_ONLY report_host='{}'".format(newname))
        print("We need to RESTART MySQL...")
        shell.get_session().run_sql("RESTART")
        time.sleep(10)
        shell.reconnect()

def _check_distributed_recovery_user():
    global recovery_user
    global recovery_password
    if not recovery_user:
        recovery_user = shell.prompt("Which user do you want to use for distributed recovery ? ")
        result = i_run_sql("select Repl_slave_priv from mysql.user where host='%' and user='{}'".format(recovery_user),"[']",False)
        if len(result) == 0:
            answer = shell.prompt("That user doesn't exist, do you want to create it ? (Y/n) ",{'defaultValue': "Y"}).upper()
            if answer != "Y":
               # aborting
               recovery_user = None
               recovery_password = None
               return False
            recovery_password2 = "a"
            while recovery_password != recovery_password2:
                recovery_password = shell.prompt("Enter the password for {} : ".format(recovery_user),{'type': 'password'})
                recovery_password2 = shell.prompt("Confirm the password for {} : ".format(recovery_user),{'type': 'password'})
                if recovery_password != recovery_password2:
                    print("Passwords don't match, try again !")
            #check is we are super read only
            result = shell.get_session().run_sql("SELECT @@super_read_only;").fetch_one()
            if result[0] == 1:
                print("ERROR: the server is running in Super Read Only Mode, aborting !")
                recovery_user = None
                recovery_password = None
                return False
            shell.get_session().run_sql("CREATE USER {} IDENTIFIED BY '{}';".format(recovery_user, recovery_password))
            shell.get_session().run_sql("GRANT REPLICATION SLAVE ON *.* TO {};".format(recovery_user))
            shell.get_session().run_sql("GRANT BACKUP_ADMIN ON *.* TO {};".format(recovery_user))
            return True
        if result[0] == "N":
            answer = shell.prompt("User {} doesn't have REPLICATION privilege, do you want to add it ? ",{'defaultValue': "Y"}).upper()
            if answer == "N":
                # aborting
                return False
            shell.get_session().run_sql("GRANT REPLICATION SLAVE ON *.* TO {};".format(recovery_user))
        result = i_run_sql("select PRIV from mysql.global_grants where host='%' and user='{}'".format(recovery_user),"[']",False)
        if result[0] == "0":
            # We don't have backup admin priv
            answer = shell.prompt("User {} doesn't have BACKUP_ADMIN privilege, do you want to add it ? ",{'defaultValue': "Y"}).upper()
            if answer == "N":
                # aborting
                return False
            shell.get_session().run_sql("GRANT BACKUP_ADMIN ON *.* TO {};".format(recovery_user))
        recovery_password = shell.prompt("Enter the password for {}: ".format(recovery_user),{'type': 'password'})
    return True


def i_run_sql(query, strdel, getColumnNames):
    session = shell.get_session()
    result = session.run_sql(query)
    list_output = []
    if (result.has_data()):
        if getColumnNames:
            list_output = [result.get_column_names()]
        for row in result.fetch_all():
             list_output.append(str(list(row)).strip(strdel))
    else:
        list_output.append("0")
    return list_output

def i_sess_identity(conn):
    clusterAdminPassword = ""
    if conn == "current":
        x = shell.parse_uri(shell.get_session().get_uri())
        hostname = x['host']
    else:
        z = shell.get_session()
        y = shell.open_session(conn)
        shell.set_session(y)
        hostname = shell.parse_uri(y.get_uri())['host']
        shell.set_session(z)
        x = shell.parse_uri(conn)
        clusterAdminPassword = x['password']
    port = x['port']
    clusterAdmin = x['user']
    return clusterAdmin, clusterAdminPassword, hostname, str(port)

@plugin_function("group_replication.status")
def status(session=None):
    """
    Check Group Replication Status.

    This function prints the status of Group Replication

    Args:
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.
    """
    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    print("Group Replication Member Status :")
    return shell.get_session().run_sql("select * from performance_schema.replication_group_members where channel_name='group_replication_applier';")

@plugin_function("group_replication.showChannel")
def showChannel(session=None):
    """
    Group Replication's Channels' Status.

    This function prints the status of Group Replication's channels

    Args:
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.
    """
    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    return shell.get_session().run_sql("Select a.channel_name, a.host, a.port, a.user, b.service_state from performance_schema.replication_connection_configuration a, performance_schema.replication_connection_status b where a.channel_name=b.channel_name")

@plugin_function("group_replication.startChannel")
def startChannel(channel_name, session=None):
    """
    Start Replication Channel

    This function starts the replication channel

    Args:
        channel_name (string): The replication channel's name.
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.
    """

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    shell.get_session().run_sql("SET GLOBAL event_scheduler = OFF;")
    shell.get_session().run_sql("alter event mysql_innodb_cluster_metadata." + channel_name + " enable;")
    shell.get_session().run_sql("SET GLOBAL event_scheduler = ON;")

@plugin_function("group_replication.stopChannel")
def stopChannel(channel_name, session=None):
    """
    Stop Replication Channel

    This function stops the replication channel

    Args:
        channel_name (string): The replication channel's name.
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.
    """

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    shell.get_session().run_sql("alter event mysql_innodb_cluster_metadata." + channel_name + " disable;")
    shell.get_session().run_sql("stop replica for channel '" + channel_name + "'")

def i_check_local_role():
    clusterAdmin, clusterAdminPassword, hostname, port = i_sess_identity("current")
    result = i_run_sql("select member_role from performance_schema.replication_group_members where channel_name='group_replication_applier' and concat(member_host,':',member_port)='" + hostname + ":" + port + "'","[']",False)
    if len(result) == 0:
        result = i_run_sql("select member_role from performance_schema.replication_group_members where channel_name='group_replication_applier' and concat(member_host,':',member_port)='127.0.0.1:" + port + "'","[']",False)
    return result[0]

def i_start_gr(isPRIMARY):
    if isPRIMARY:
        result = i_run_sql("SET GLOBAL group_replication_bootstrap_group=ON","",False)
        result = i_run_sql("START GROUP_REPLICATION;", "", False)
        result = i_run_sql("SET GLOBAL group_replication_bootstrap_group=OFF","",False)
    else:
        result = i_run_sql("START GROUP_REPLICATION;","",False)

def i_get_other_node():
    clusterAdmin, clusterAdminPassword, hostname, port = i_sess_identity("current")
    result = i_run_sql("show variables like 'group_replication_group_seeds'","[']",False)
    host_list = result[0].strip("group_replication_group_seeds',").strip(" '").replace(",", " ").split()
    result = []
    for node in host_list:
        if node != ("{}:{}".format(hostname, int(port) + 10)):
            result.append(node)
    return result

def i_comp_gtid(gtid1, gtid2):
    result = i_run_sql("select gtid_subset('" + gtid2 + "','" + gtid1 + "')","[`]", False)
    return result[0]

def i_get_gtid(connectionStr):
    if connectionStr != "current":
        x=shell.open_session(connectionStr)
        shell.set_session(x)
    result = i_run_sql("show variables like 'gtid_executed'","[`]",False)
    return result[0].replace("'gtid_executed', '","").replace("'","")

def i_drop_ic_metadata():
    result = i_run_sql("drop database if exists mysql_innodb_cluster_metadata;","[']",False)

def i_stop_other_replicas():
    result = i_run_sql("select channel_name from performance_schema.replication_connection_configuration where CHANNEL_NAME not like 'group_replication_%'","",False)
    if len(result) != 0:
        for channelName in result:
            stop_other_replicas = i_run_sql("stop replica for channel '" + channelName + "'","",False)

def i_check_group_replication_recovery():
    result = i_run_sql("select count(1) from performance_schema.replication_connection_status where channel_name='group_replication_recovery'","[']", False)
    return result[0]

def i_get_gr_seed():
    result = i_run_sql("show variables like 'group_replication_group_seeds'","['group_replication_group_seeds'",False)
    return result[0].strip(", '").strip("']")

def i_set_grseed_replicas(gr_seed, clusterAdmin):
    global clusterAdminPassword
    global recovery_user
    global recovery_password
    i_run_sql("set persist group_replication_group_seeds='" + gr_seed + "'","[']",False)
    if not recovery_user:
        result = _check_distributed_recovery_user()
        if not result:
            return False
    i_run_sql("CHANGE MASTER TO MASTER_USER='{}', MASTER_PASSWORD='{}' FOR CHANNEL 'group_replication_recovery';".format(recovery_user, recovery_password),"[']",False)
    return True

def i_set_all_grseed_replicas(gr_seed, new_gr_seed, clusterAdmin, clusterAdminPassword):
    x=shell.get_session()
    for node in i_get_other_node():
        host, port = node.split(':')
        y=shell.open_session("{}@{}:{}".format(shell.parse_uri(x.get_uri())['user'],host, int(port)-10))
        shell.set_session(y)
        i_install_plugin("group_replication", "group_replication.so")
        i_set_grseed_replicas(new_gr_seed, shell.parse_uri(x.get_uri())['user'] )
    shell.set_session(x)
    i_set_grseed_replicas(new_gr_seed, shell.parse_uri(x.get_uri())['user'])

def i_get_host_port(connectionStr):
    if (connectionStr.find("@") != -1):
        connectionStr = connectionStr.replace("@", " ").split()[1]
    if (connectionStr.find("localhost") != -1 or connectionStr.find("127.0.0.1") != -1):
        clusterAdmin, clusterAdminPassword, hostname, port = i_sess_identity("current")
        port = connectionStr.replace(":"," ").split()[1]
        connectionStr = hostname + ":" + port
    return connectionStr

def setPrimaryInstance(connectionStr):
    connectionStr = i_get_host_port(connectionStr)
    new_primary = i_run_sql("SELECT member_id FROM performance_schema.replication_group_members where channel_name='group_replication_applier' and concat(member_host,':',member_port)='" + connectionStr + "'","[']",False)
    if len(new_primary) == 0:
        new_primary = i_run_sql("SELECT member_id FROM performance_schema.replication_group_members where channel_name='group_replication_applier' and concat(member_host,':',member_port)='127.0.0.1:" + str(shell.parse_uri(connectionStr)['port']) +"'","[']",False)
    result = i_run_sql("select group_replication_set_as_primary('" + new_primary[0] + "')","'",False)
    return status()

def i_start_gr_all(connectionStr):
    x=shell.get_session()
    clusterAdmin, clusterAdminPassword, hostname, port = i_sess_identity(connectionStr)
    i_start_gr(True)
    for node in i_get_other_node():
        y=shell.open_session(clusterAdmin + ":" + clusterAdminPassword + "@" + node)
        shell.set_session(y)
        i_start_gr(False)
    shell.set_session(x)

def i_create_or_add(ops, connectionStr, group_replication_group_name, group_replication_group_seeds):
    global clusterAdminPassword
    clusterAdmin = shell.parse_uri(shell.get_session().get_uri())['user']
    if not clusterAdminPassword:
       clusterAdminPassword = shell.prompt("Enter the password for {} : ".format(connectionStr),{'type': 'password'})
    if (ops == "ADD" or ops == "CLONE"):
        CA, CAP, local_hostname, local_port = i_sess_identity("current")
        x=shell.get_session()
        y = shell.open_session(connectionStr)
        clusterAdmin = shell.parse_uri(y.get_uri())['user']
        shell.set_session(y)
        _check_report_host()
    i_install_plugin("group_replication", "group_replication.so")
    result = i_run_sql("set persist group_replication_group_name='" + group_replication_group_name + "'","[']",False)
    result = i_run_sql("set persist group_replication_start_on_boot='ON'","[']",False)
    result = i_run_sql("set persist group_replication_bootstrap_group=off","[']",False)
    result = i_run_sql("set persist group_replication_recovery_use_ssl=1","[']",False)
    result = i_run_sql("set persist group_replication_ssl_mode='REQUIRED'","[']",False)
    result = i_run_sql("set persist group_replication_local_address='{}:{}'".format(shell.parse_uri(shell.get_session().get_uri())['host'], int(shell.parse_uri(shell.get_session().get_uri())['port'])+10),"[']",False)
    result = i_set_grseed_replicas(group_replication_group_seeds, clusterAdmin)
    if not result:
        return False
    if ops == "CLONE":
        i_clone(local_hostname + ":" + local_port,clusterAdmin,clusterAdminPassword)
        # i_remove_plugin("clone")
    if ops == "CREATE":
        i_start_gr(True)
    else:
        if ops == "ADD":
            i_start_gr(False)
        shell.set_session(x)
        #if ops == "CLONE":
        #    i_remove_plugin("clone")
    return True

def i_define_gr_name():
    result = i_run_sql("select uuid()","[']",False)
    return result[0]

def i_get_gr_name():
    result = i_run_sql("show variables like 'group_replication_group_name'","['group_replication_group_name'",False)
    return result[0].strip(", '").strip("']")

def i_install_plugin(plugin_name, plugin_file):
    result = i_run_sql("select count(1) from information_schema.plugins where plugin_name='" + plugin_name + "'","[']", False)
    if result[0] == "0":
        result = i_run_sql("INSTALL PLUGIN " + plugin_name + " SONAME '" + plugin_file + "';","[']",False)

def i_remove_plugin(plugin_name):
    result = i_run_sql("select count(1) from information_schema.plugins where plugin_name='" + plugin_name + "'","[']", False)
    if result[0] != "0":
        result = i_run_sql("uninstall PLUGIN " + plugin_name + ";","[']",False)

def i_clone(source, cloneAdmin, cloneAdminPassword):
    clusterAdmin, clusterAdminPassword, hostname, port = i_sess_identity("current")
    i_install_plugin("clone", "mysql_clone.so")
    result = i_run_sql("set global clone_valid_donor_list='" + source + "';","", False)
    print("Clone database is started ...")
    result = i_run_sql("set global super_read_only=off;","", False)
    result = i_run_sql("clone instance from " + cloneAdmin + "@" + source + " identified by '" + cloneAdminPassword + "'", "", False)
    time.sleep(10)

@plugin_function("group_replication.create")
def create():
    """
    Create MySQL Group Replication

    This function creates a Group Replication environment

    """
    _check_report_host()
    clusterAdmin, clusterAdminPassword, hostname, port = i_sess_identity("current")
    gr_seed = "{}:{}".format(hostname, int(port) + 10)
    result = i_create_or_add("CREATE",gr_seed, i_define_gr_name(), gr_seed)
    if result:
        return status()
    else:
        print("ERROR: Group Replication Creation Aborted !")
        return

@plugin_function("group_replication.addInstance")
def addInstance(connectionStr):
    """
    Add instance to group replication

    This function adds an instance to an existing Group Replication

    Args:
        connectionStr (string): uri clusterAdmin:clusterAdminPassword@hostname:port
    """
    clusterAdmin, clusterAdminPassword, hostname, port = i_sess_identity("current")
    print("A new instance will be added to the Group Replication. Depending on the amount of data on the group this might take from a few seconds to several hours.")
    print(" ")
    #if (connectionStr.count(':') == 1):
    #    clusterAdminPassword = shell.prompt('Please provide the password for ' + connectionStr + ': ',{"type":"password"})
    #else:
    #    clusterAdminPassword = ((connectionStr.replace(":"," ")).replace("@", " ")).split()[1]
    clone_sts = shell.prompt("Please select a recovery method [C]lone/[I]ncremental recovery/[A]bort (default Clone): ").upper()
    if (clone_sts == "C" or clone_sts == "" or clone_sts == " "):
        clone_sts = "CLONE"
    else:
        if clone_sts == "I":
            clone_sts = "ADD"
        else:
            clone_sts = "A"
            print("Adding instance is aborted")
    if clone_sts != "A":
        old_gr_seed = i_get_gr_seed()
        #add_gr_node = "{}:{}".format(i_get_host_port(connectionStr).split(':')[0], int(i_get_host_port(connectionStr).split(':')[1]))
        add_gr_node = connectionStr
        add_gr_seed = "{}:{}".format(i_get_host_port(connectionStr).split(':')[0], int(i_get_host_port(connectionStr).split(':')[1])+10)
        if old_gr_seed.find(add_gr_seed) != -1:
            new_gr_seed = old_gr_seed
        else:
            new_gr_seed = old_gr_seed + "," + add_gr_seed
        if clone_sts == "CLONE":
            i_install_plugin("clone", "mysql_clone.so")
        i_set_all_grseed_replicas(old_gr_seed, new_gr_seed, clusterAdmin, clusterAdminPassword)
        i_create_or_add(clone_sts,add_gr_node,i_get_gr_name(), new_gr_seed)
    return status()

@plugin_function("group_replication.convertToIC")
def convertToIC(clusterName):
    """
    Convert From Group Replication to InnoDB Cluster

    This function converts a Group Replication environment to MySQL InnoDB Cluster

    Args:
        clusterName (string): Any name for your InnoDB Cluster
    """
    msg_output = "0"
    if i_check_local_role() == "PRIMARY":
        i_drop_ic_metadata()
        i_stop_other_replicas()
        dba.create_cluster(clusterName, {"adoptFromGR":True})
        msg_output = "Successful conversion from Group Replication to InnoDB Cluster"
    else:
        msg_output = "FAILED - Instance is not PRIMARY"
    return msg_output

@plugin_function("group_replication.adoptFromIC")
def adoptFromIC():
    """
    Convert From InnoDB Cluster to native Group Replication

    This function converts a MySQL InnoDB Cluster to a native Group Replication environment

    """
    msg_output = "FAILED - Instance is not a PRIMARY"
    if i_check_local_role() == "PRIMARY":
       clusterAdmin, clusterAdminPassword, hostname, port = i_sess_identity("current")
       clusterAdminPassword = shell.prompt('Please provide password for Cluster Admin: ',{"type":"password"})
       host_list = i_get_other_node()
       dba.get_cluster().dissolve({"interactive":False})
       create()
       if len(host_list) != 0:
           for secNode in host_list:
               addInstance(clusterAdmin + ":" + clusterAdminPassword + "@" + secNode)
       i_drop_ic_metadata()
       msg_output = "Successful conversion from InnoDB Cluster to Group Replication"
    return msg_output

@plugin_function("group_replication.rebootGRFromCompleteOutage")
def rebootGRFromCompleteOutage():
   """
   Startup Group Replication

   This function starts Group Replication

   """
   clusterAdmin, clusterAdminPassword, hostname, port = i_sess_identity("current")
   clusterAdminPassword = shell.prompt('Please provide password for Cluster Admin : ',{"type":"password"})
   x=shell.get_session()
   local_gtid = i_get_gtid("current")
   process_sts = "Y"
   for node in i_get_other_node():
       remote_gtid = i_get_gtid(clusterAdmin + ":" + clusterAdminPassword + "@" + node)
       if i_comp_gtid(local_gtid, remote_gtid) != "1":
            process_sts = "N"
   if process_sts == "Y":
       shell.set_session(x)
       print("This node is suitable to start Group Replication")
       print("Process may take a while ...")
       i_start_gr_all(clusterAdmin + ":" + clusterAdminPassword + "@" + hostname + ":" + port)
       print("Reboot Group Replication process is completed")
   else:
       print("Node was not a PRIMARY, try another node")
   return status()

@plugin_function("group_replication.replicateFromIC")
def replicateFromIC(channel_name, router_host, router_port_number, session=None):
    """
    Convert From Group Replication to InnoDB Cluster

    This function converts a Group Replication environment to MySQL InnoDB Cluster

    Args:
        channel_name (string): Any name for your InnoDB Cluster
        router_host (string): InnoDB Cluster router host
        router_port_number (integer): InnoDB Cluster router port
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.
    """
    import time

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    router_port = str(router_port_number)
    clusterAdmin, clusterAdminPassword, hostname, port = i_sess_identity("current")
    clusterAdminPassword = shell.prompt('Please provide password for Cluster Admin : ',{"type":"password"})
    for node in i_get_other_node():
        print("Configure replication channel on '" + node)
        y=shell.open_session(clusterAdmin + ":" + clusterAdminPassword + "@" + node)
        shell.set_session(y)
        result = i_run_sql("change master to master_host='" + router_host + "', master_port=" + router_port + ", master_user='" + clusterAdmin + "', master_password='" + clusterAdminPassword + "', master_ssl=1, master_auto_position=1, get_master_public_key=1 for channel '" + channel_name + "'", "", False)
    shell.set_session(session)
    print("Configure replication channel on " + hostname + ":" + port)
    result = i_run_sql("change master to master_host='" + router_host + "', master_port=" + router_port + ", master_user='" + clusterAdmin + "', master_password='" + clusterAdminPassword + "', master_ssl=1, master_auto_position=1, get_master_public_key=1 for channel '" + channel_name + "'", "", False)
    print("Starting replication from " + router_host + ":" + router_port + " ...")
    result = i_run_sql("start replica for channel '" + channel_name + "'", "", False)
    time.sleep(10)
    result = i_run_sql("create event mysql_innodb_cluster_metadata." + channel_name + " ON SCHEDULE every 2 second do start replica for channel '" + channel_name + "';", "", False)
    return print("All nodes have this replication channel")
