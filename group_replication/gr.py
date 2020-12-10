from mysqlsh.plugin_manager import plugin, plugin_function



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
        hostname = (i_run_sql("select @@hostname;","[']", False))[0]
    else:
        z = shell.get_session()
        y = shell.open_session(conn)
        shell.set_session(y)
        hostname = (i_run_sql("select @@hostname;","[']", False))[0]
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
    shell = mysqlsh.globals.shell

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
    shell = mysqlsh.globals.shell

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

    shell = mysqlsh.globals.shell

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
    shell = mysqlsh.globals.shell

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
        if node != (hostname + ":" + port + "1"):
            result.append(node[:-1])
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

def i_set_grseed_replicas(gr_seed, clusterAdmin, clusterAdminPassword):
    result = i_run_sql("set persist group_replication_group_seeds='" + gr_seed + "'","[']",False)
    if clusterAdminPassword == "":
        result = i_run_sql("CHANGE MASTER TO MASTER_USER='" + clusterAdmin + "' FOR CHANNEL 'group_replication_recovery';","[']",False)
    else:
        if i_check_group_replication_recovery() == "0":
            result = i_run_sql("CHANGE MASTER TO MASTER_USER='" + clusterAdmin + "', MASTER_PASSWORD='" + clusterAdminPassword + "' FOR CHANNEL 'group_replication_recovery';","[']",False)

def i_set_all_grseed_replicas(gr_seed, new_gr_seed, clusterAdmin, clusterAdminPassword):
    x=shell.get_session()
    for node in i_get_other_node():
        y=shell.open_session(clusterAdmin + ":" + clusterAdminPassword + "@" + node)
        shell.set_session(y)
        i_set_grseed_replicas(new_gr_seed, clusterAdmin, clusterAdminPassword)
    shell.set_session(x)
    i_set_grseed_replicas(new_gr_seed, clusterAdmin, clusterAdminPassword)

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

def i_create_or_add(ops, connectionStr, clusterAdmin, clusterAdminPassword, group_replication_group_name, group_replication_group_seeds):
    if (ops == "ADD" or ops == "CLONE"):
        CA, CAP, local_hostname, local_port = i_sess_identity("current")
        x=shell.get_session()
        y = shell.open_session(clusterAdmin + ":" + clusterAdminPassword + "@" + connectionStr[:-1])
        shell.set_session(y)
    i_install_plugin("group_replication", "group_replication.so")
    result = i_run_sql("set persist group_replication_group_name='" + group_replication_group_name + "'","[']",False)
    result = i_run_sql("set persist group_replication_start_on_boot='ON'","[']",False)
    result = i_run_sql("set persist group_replication_bootstrap_group=off","[']",False)
    result = i_run_sql("set persist group_replication_ssl_mode='REQUIRED'","[']",False)
    result = i_run_sql("set persist group_replication_local_address='" + connectionStr + "'","[']",False)
    i_set_grseed_replicas(group_replication_group_seeds, clusterAdmin, clusterAdminPassword)
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
    return status()

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
    import time
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

    clusterAdmin, clusterAdminPassword, hostname, port = i_sess_identity("current")
    gr_seed = hostname + ":" + port + "1"
    i_create_or_add("CREATE",gr_seed,clusterAdmin, clusterAdminPassword, i_define_gr_name(), gr_seed)
    return status()

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
    if (connectionStr.count(':') == 1):
        clusterAdminPassword = shell.prompt('Please provide the password for ' + connectionStr + ': ',{"type":"password"})
    else:
        clusterAdminPassword = ((connectionStr.replace(":"," ")).replace("@", " ")).split()[1]
    clone_sts = shell.prompt("Please select a recovery method [C]lone/[I]ncremental recovery/[A]bort (default Clone): ")
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
        add_gr_seed = i_get_host_port(connectionStr) + "1"
        if old_gr_seed.find(add_gr_seed) != -1:
            new_gr_seed = old_gr_seed
        else:
            new_gr_seed = old_gr_seed + "," + add_gr_seed
        if clone_sts == "CLONE":
            i_install_plugin("clone", "mysql_clone.so")
        i_set_all_grseed_replicas(old_gr_seed, new_gr_seed, clusterAdmin, clusterAdminPassword)
        i_create_or_add(clone_sts,add_gr_seed,clusterAdmin,clusterAdminPassword,i_get_gr_name(), new_gr_seed)
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

    shell = mysqlsh.globals.shell

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
