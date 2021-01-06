from mysqlsh.plugin_manager import plugin, plugin_function
import mysqlsh
import time

shell = mysqlsh.globals.shell
dba = mysqlsh.globals.dba

clusterAdminPassword = None
recovery_user = None
recovery_password = None
convert_to_gr = False
autoFlipProcess = None
remote_user = None
remote_password = None
autoCloneProcess = None

#################################################3
#
#
#   BASIC AND COMMON FUNCTIONS:
#
#

def _check_report_host():
    global autoFlipProcess

    if not autoFlipProcess:
        result = shell.get_session().run_sql("SELECT @@report_host;").fetch_one()
        answer = shell.prompt("\nThe server reports as [{}], is this correct ? (Y/n) ".format(result[0]),{'defaultValue': "Y"}).upper()
        if answer != 'Y':
            newname = shell.prompt("Enter the hostname that should be used as report_host: ")
            shell.get_session().run_sql("SET PERSIST_ONLY report_host='{}'".format(newname))
            print("We need to RESTART MySQL...")
            try:
                shell.get_session().run_sql("RESTART")
                time.sleep(10)
                shell.reconnect()
                return True
            except:
                print("\n\033[1mERROR:\033[0m Restart server failed (mysqld is not managed by supervisor process)\n")
                return False
        else:
            return True
    else:
        return True


def _check_distributed_recovery_user():
    global recovery_user
    global recovery_password
    if not recovery_user:
        recovery_user = shell.prompt("\nWhich user do you want to use for distributed recovery ? ")
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
        result = i_run_sql("show variables like 'report_host'","[']",False)
        hostname = result[0].strip("report_host'").strip(", '")
        if len(hostname.strip()) == 0:
           result = i_run_sql("select @@hostname", "[']", False)
           hostname = result[0]
        x = shell.parse_uri(shell.get_session().get_uri())
    else:
        z = shell.get_session()
        y = shell.open_session(conn)
        shell.set_session(y)
        result = i_run_sql("show variables like 'report_host'","[']",False)
        hostname = result[0].strip("report_host'").strip(", '")
        shell.set_session(z)
        if len(hostname.strip()) == 0:
           result = i_run_sql("select @@hostname", "[']", False)
           hostname = result[0]
        x = shell.parse_uri(conn)
        clusterAdminPassword = x['password']
    port = x['port']
    clusterAdmin = x['user']
    return clusterAdmin, clusterAdminPassword, hostname, str(port)

def i_check_local_role():
    clusterAdmin, clusterAdminPassword, hostname, port = i_sess_identity("current")
    result = i_run_sql("select member_role from performance_schema.replication_group_members where channel_name='group_replication_applier' and concat(member_host,':',member_port)='" + hostname + ":" + port + "'","[']",False)
    return result[0]

def i_start_gr(isPRIMARY):
    try:
        if isPRIMARY:
            result = i_run_sql("SET GLOBAL group_replication_bootstrap_group=ON","",False)
            result = i_run_sql("START GROUP_REPLICATION;", "", False)
            result = i_run_sql("SET GLOBAL group_replication_bootstrap_group=OFF","",False)
        else:
            result = i_run_sql("START GROUP_REPLICATION;","",False)
    except:
        print("\033[1mINFO: \033[0m Unable to start group replication on this node, SKIPPED!")

def i_get_other_node():
    clusterAdmin, foo, hostname, port = i_sess_identity("current")
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

def i_get_gtid():
    result = i_run_sql("show variables like 'gtid_executed'","[`]",False)
    return result[0].replace("'gtid_executed', '","").replace("'","")

def i_drop_ic_metadata():
    result = i_run_sql("drop database if exists mysql_innodb_cluster_metadata;","[']",False)

def i_list_all_channel():
    result = i_run_sql("select event_name from information_schema.events where status='ENABLED' and event_name in (select channel_name from performance_schema.replication_connection_status where channel_name not like '%group_replication_%' and service_state='ON');","[']",False)
    return result

def i_stop_other_replicas():
    if len(i_list_all_channel()) != 0:
        for channelName in i_list_all_channel():
            stopMultiClusterChannel(channelName)

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

        print("\n\033[1mConfiguring node '" + host + ":" + port + ":\033[0m")
        try:
            y=shell.open_session("{}@{}:{}".format(shell.parse_uri(x.get_uri())['user'],host, int(port)-10), clusterAdminPassword)
            shell.set_session(y)
        except:
            print("\033[1mINFO: \033[0m Unable to connect to '" + host + ":" + port + "', SKIPPED!")
        i_install_plugin("group_replication", "group_replication.so")
        i_set_grseed_replicas(new_gr_seed, shell.parse_uri(x.get_uri())['user'] )
    shell.set_session(x)
    i_set_grseed_replicas(new_gr_seed, shell.parse_uri(x.get_uri())['user'])
    print("\n\033[1mAll nodes are set to work with this new node \033[0m\n")

def i_get_host_port(connectionStr):
    if (connectionStr.find("@") != -1):
        connectionStr = connectionStr.replace("@", " ").split()[1]
    if (connectionStr.find("localhost") != -1 or connectionStr.find("127.0.0.1") != -1):
        clusterAdmin, clusterAdminPassword, hostname, port = i_sess_identity("current")
        port = connectionStr.replace(":"," ").split()[1]
        connectionStr = hostname + ":" + port
    return connectionStr

def i_start_gr_all(clusterAdmin, clusterAdminPassword):
    x=shell.get_session()
    i_start_gr(True)
    for node in i_get_other_node():
        if shell.parse_uri(node)["port"] > 10000:
            n = node[:-1]
        else:
            n = shell.parse_uri(node)["host"] + ":" + str(shell.parse_uri(node)["port"] - 10)
        try:
            print("\033[1mINFO: \033[0m Starting Group Replication on '" + node + "'")
            y=shell.open_session(clusterAdmin + "@" + n, clusterAdminPassword)
            shell.set_session(y)
            i_start_gr(False)
        except:
            print("\033[1mINFO: \033[0m Unable to connect to '" + node + "', SKIPPED!")
    shell.set_session(x)

def i_create_or_add(ops, connectionStr, group_replication_group_name, group_replication_group_seeds):
    global clusterAdminPassword
    clusterAdmin = shell.parse_uri(shell.get_session().get_uri())['user']
    if not clusterAdminPassword:
       clusterAdminPassword = shell.prompt("Enter the password for {} : ".format(connectionStr),{'type': 'password'})

    if (ops == "ADD" or ops == "CLONE"):
        CA, CAP, local_hostname, local_port = i_sess_identity("current")
        x=shell.get_session()
        try:
            y = shell.open_session(connectionStr, clusterAdminPassword)
        except:
            print("\033[1mINFO: \033[0m Unable to convert '" + connectionStr + "', SKIPPED!")
            return
        clusterAdmin = shell.parse_uri(y.get_uri())['user']
        shell.set_session(y)
        if not _check_report_host():
            return
        result = i_run_sql('set global super_read_only=off',"[']",False)
    i_install_plugin("group_replication", "group_replication.so")
    result = i_run_sql("set persist group_replication_group_name='" + group_replication_group_name + "'","[']",False)
    result = i_run_sql("set persist group_replication_start_on_boot='ON'","[']",False)
    result = i_run_sql("set persist group_replication_bootstrap_group=off","[']",False)
    result = i_run_sql("set persist group_replication_recovery_use_ssl=1","[']",False)
    result = i_run_sql("set persist group_replication_ssl_mode='REQUIRED'","[']",False)
   
    # Bug: GR instance adding process hanging on server with multiple network interface
    print("\n\033[1mWARNING:\033[0m Adding Instance may fail for server with multiple network interface")
    print("It is strongly suggested to set group_replication_ip_allowlist (example : 10.0.0.0/8)")
    ip_whitelist = shell.prompt("Please enter the value for group_replication_ip_allowlist : ")
 
    try:
        result = i_run_sql("set persist group_replication_ip_allowlist = '" + ip_whitelist + "'","",False)
    except:
       try:
         print("Set group_replication_ip_whitelist to " + ip_whitelist + "\n")
         result = i_run_sql("set persist group_replication_ip_whitelist = '" + ip_whitelist + "'", "", False)
       except:
         print("Unable to set group_replication_ip_allowlist nor group_replication_ip_whitelist")

    # Bug: when connecting to localhost, system gave wrong group_replication_local_Address when creating GR
    hostname = shell.parse_uri(shell.get_session().get_uri())['host']
    if len(hostname.strip()) == 0 or hostname == 'localhost':
           result = i_run_sql("select @@hostname", "[']", False)
           hostname = result[0]
    result = i_run_sql("set persist group_replication_local_address='{}:{}'".format(hostname, int(shell.parse_uri(shell.get_session().get_uri())['port'])+10),"[']",False)


    # result = i_run_sql("set persist group_replication_local_address='{}:{}'".format(shell.parse_uri(shell.get_session().get_uri())['host'], int(shell.parse_uri(shell.get_session().get_uri())['port'])+10),"[']",False)
    
    result = i_set_grseed_replicas(group_replication_group_seeds, clusterAdmin)

    if not result:
        return False
    if ops == "CLONE":
        print("\n\033[1mINFO:\033[0m Clone to " + connectionStr)
        i_clone(local_hostname + ":" + local_port,clusterAdmin,clusterAdminPassword)
        # i_remove_plugin("clone")
    if ops == "CREATE":
        i_start_gr(True)
    else:
        if ops == "ADD":
            print("\n\033[1mINFO:\033[0m Start incremental recovery on " + connectionStr)
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
    _host = shell.parse_uri(cloneAdmin + '@' + source)['host']
    _port = shell.parse_uri(cloneAdmin + '@' + source)['port']
    result = i_run_sql("clone instance from " + cloneAdmin + "@'" + _host + "':" + str(_port) + " identified by '" + cloneAdminPassword + "'", "", False)
    
    # Bug: to add checking instance status for environment that needs more than 10 seconds to RESTART
    # time.sleep(10)
    restart_status = False
    while not restart_status:
        time.sleep(10)
        shell.reconnect()
        connect_status = str(shell.status())
        if connect_status.find('Not Connected') == -1:
            restart_status = True 


def i_checkIfAsyncFailoverImplemented(channel_name):
    try:
        result = i_run_sql("select repl_user from mysql_gr_replication_metadata.channel;","[']",False)
        repl_user=result[0]
    except:
        print("\n\033[1mINFO:\033[0m Multi Cluster replication is not set \n")
        return False
    r = i_run_sql("select count(1)  from mysql.replication_asynchronous_connection_failover where channel_name='" + channel_name + "'", "[']",False)
    if r[0] == "0":
        return False
    else:
        return True

def i_checkInstanceConfiguration():
    print("\033[1mValidating MySQL instance for use in a Group Replication...\033[0m\n")

    result = i_run_sql("show grants", "[']", False)
    user_ready=False
    for i in range(len(result)):
        if "mysql_innodb_cluster_metadata" in result[i]:
            user_ready=True
    if not user_ready:
        print('\n\033[1mERROR:\033[0m This user is not ready for Cluster, please run dba.configureInstance if you have not run this \n')
        return False
    else:
        print('\nThis user is suiteable for configuring a Group Replication ... \n')

    result = i_run_sql("show variables like 'server_id'","[']",False)
    server_id=result[0].strip("server_id', '")
    if int(server_id) < 2:
        print("\033[1mERROR:\033[0m Current server_id is " + server_id + ", please set a unique number for server_id greater than 1")
    result = i_run_sql("show variables like 'gtid_mode'","[']",False)
    gtid_mode=result[0].strip("gtid_mode', '")
    if gtid_mode != 'ON':
        print("\033[1mERROR:\033[0m Current GTID_MODE is " + gtid_mode + ", please set GTID_MODE to 'ON'")
    result = i_run_sql("show variables like 'enforce_gtid_consistency'","[']",False)
    enforce_gtid_consistency=result[0].strip("enforce_gtid_consistency', '")
    if enforce_gtid_consistency != 'ON':
        print("\033[1mERROR:\033[0m Current enforce-gtid-consistency is " + enforce_gtid_consistency + ", please set enforce-gtid-consistency to 'ON'")
    if (int(server_id) > 1 and gtid_mode=='ON' and enforce_gtid_consistency=='ON'):
        print("\n\033[1mINFO:\033[0m System variables are ready for a group replication")
        print("\n\033[1mINFO:\033[0m To avoid ISSUES, you must run your instance with skip-slave-start=ON")
        print("\033[1mINFO:\033[0m Consider to put skip-slave-start on the option file (my.cnf) \n")
        return True
    else:
        return False


#################################
#
#  GROUP REPLICATION FUNCTIONS
#
#

@plugin_function("group_replication.status")
def status(session=None):
    """
    Check Group Replication Status.
    This function prints the status of Group Replication
    Args:
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.
    """
    try:
        dba.get_cluster()
        print("\n\033[1mINFO:\033[0m InnoDB Cluster \n")
        return
    except:
        try:
            result = i_run_sql("show variables like 'group_replication_group_name'","[']",False)
            z = result[0].strip("group_replication_group_name', '")
            print("\n\033[1mINFO:\033[0m Group Replication Group Name : " + z)
        except:
            print("\n\033[1mERROR:\033[0m Group Replication is not configured \n")
            return

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    print("\n\033[1mRegistered Members on this node :\033[0m")
    print(shell.parse_uri(session.get_uri())["host"] + ":" + str(shell.parse_uri(session.get_uri())["port"]))
    try:
        host_list = i_get_other_node()
        if len(host_list) != 0:
            for secNode in host_list:
                if shell.parse_uri(secNode)["port"] > 10000:
                    print(secNode[:-1])
                else:
                    print(shell.parse_uri(secNode)["host"] + ":" + str(shell.parse_uri(secNode)["port"] - 10))
    except:
        print("\033[1mINFO:\033[0m Failed to retrieve all members")
    print("\n\033[1mGroup Replication Member Status :\033[0m")
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

    try:
        result = i_run_sql("select channel_name, host, port, weight from mysql.replication_asynchronous_connection_failover order by channel_name, host, port","", False)
        if len(result)>0:
            print("\n\033[1mReplication Asynchronous Connection Failover Nodes: \033[0m\n")
            for row in range(len(result)):
                print(result[row])
        else:
            print("\n\033[1mINFO:\033[0m Replication Asynchronous Connection Failover is not IMPLEMENTED !\n")
    except:
        print("\n\033[1mINFO:\033[0m Replication Asynchronous Connection Failover requires minimum 8.0.22\n")

    print("\n\033[1mChannel Status: \033[0m\n")
    return shell.get_session().run_sql("Select a.channel_name, a.host, a.port, a.user, b.service_state Replica_IO, c.service_state Replica_SQL from performance_schema.replication_connection_configuration a, performance_schema.replication_connection_status b, performance_schema.replication_applier_status c where a.channel_name=b.channel_name and a.channel_name=c.channel_name")

@plugin_function("group_replication.setPrimaryInstance")
def setPrimaryInstance(connectionStr):
    """
    Set PRIMARY instance for the Group Replication.
    This function sets a node in the Group Replication to be a PRIMARY instance 
    Args:
        connectionStr (string): uri clusterAdmin:clusterAdminPassword@hostname:port
    """
    x=shell.get_session()
    connectionStr = i_get_host_port(connectionStr)

    if i_check_local_role() != "PRIMARY":
        current_primary = i_run_sql("select concat(member_host,':',member_port) from performance_schema.replication_group_members where channel_name='group_replication_applier' and member_role='PRIMARY'","[']",False)
        cp_user=shell.prompt('Please provide user to connect to current PRIMARY node (' + current_primary[0] + '): ')
        # pp_user=shell.prompt('Please provide password to connect to current PRIMARY node (' + current_primary[0] + '): ', {"type":"password"})
        try:
            c=shell.open_session(cp_user + '@' + current_primary[0])
            shell.set_session(c)
        except:
            shell.set_session(x)
            print('ERROR: unable to connect to PRIMARY server !')
            return

    new_primary = i_run_sql("SELECT member_id FROM performance_schema.replication_group_members where channel_name='group_replication_applier' and concat(member_host,':',member_port)='" + connectionStr + "'","[']",False)

    if len(new_primary) == 0:
        new_primary = i_run_sql("SELECT member_id FROM performance_schema.replication_group_members where channel_name='group_replication_applier' and concat(member_host,':',member_port)='127.0.0.1:" + str(shell.parse_uri(connectionStr)['port']) +"'","[']",False)

    start_channel_name=[]
    try:
        if len(i_list_all_channel()) != 0:
            c_user=shell.prompt('Please provide user to start replication channel on ' + connectionStr + ': ')
            # p_user=shell.prompt('Please provide password for ' + c_user + ': ', {"type":"password"})
            for channel_name in i_list_all_channel():
                stopMultiClusterChannel(channel_name)
                start_channel_name.append(channel_name)
            y=shell.open_session(c_user + "@" + connectionStr)
            shell.set_session(y)
            result = i_run_sql("select group_replication_set_as_primary('" + new_primary[0] + "')","'",False)
            for channel_name in start_channel_name:
                startMultiClusterChannel(channel_name)
            shell.set_session(x)
        else:
            result = i_run_sql("select group_replication_set_as_primary('" + new_primary[0] + "')","'",False)

    except:
        shell.set_session(x)
        if len(start_channel_name) != 0:
            for channel_name in start_channel_name:
                startMultiClusterChannel(channel_name)
        print("ERROR: the server is not part of Group Replication or is not running or unable to connect to the current PRIMARY server, aborting !")

    shell.set_session(x)

    return status()

@plugin_function("group_replication.removeInstance")
def removeInstance(connectionStr):
    """
    Remove instance from group replication
    This function remove an instance from an existing Group Replication
    Args:
        connectionStr (string): uri clusterAdmin:clusterAdminPassword@hostname:port
    """
    global clusterAdminPassword
    global autoFlipProcess

    if not autoFlipProcess:
        p = shell.prompt("\nDo you want to remove '" + connectionStr + "' from the Group Replication ? (y/N) ",{'defaultValue': "N"}).upper()
    else:
        p = "Y"

    clusterAdmin, foo, hostname, port = i_sess_identity("current")
    if not clusterAdminPassword:
        try:
           if shell.parse_uri(connectionStr)['password'] == "":
              clusterAdminPassword = shell.prompt("Please provide the password for \033[96m'" + clusterAdmin + "'\033[0m : ", {"type":"password"}) 
           else:
              clusterAdminPassword=shell.parse_uri(connectionStr)['password']
        except:
           clusterAdminPassword = shell.prompt("Please provide the password for \033[96m'" + clusterAdmin + "'\033[0m : ", {"type":"password"})

    if p == "Y":
        connectionStr = i_get_host_port(connectionStr)
        print("\n\033[1mINFO:\033[0m Removing instance '" + connectionStr + "'")
        local_instance = shell.get_session()
        try:
            remote_instance = shell.open_session(clusterAdmin + "@" + connectionStr, clusterAdminPassword)
            shell.set_session(remote_instance)
            report_host = i_run_sql("show variables like 'report_host'","[']", False)
            report_port = i_run_sql("show variables like 'report_port'","[']", False)
            shell.set_session(local_instance)
            
            check = i_run_sql("select count(1) from performance_schema.replication_group_members where channel_name='group_replication_applier' and member_host='" +  report_host[0].strip("report_host', '") + "' and member_port=" + report_port[0].strip("report_port', '"), "[']", False)
            if check != "0":
                shell.set_session(remote_instance)
                result = i_run_sql("stop group_replication","",False)
                result = i_run_sql("reset persist group_replication_group_name","[']",False)
                result = i_run_sql("reset persist group_replication_start_on_boot","[']",False)
                result = i_run_sql("reset persist group_replication_bootstrap_group","[']",False)
                result = i_run_sql("reset persist group_replication_local_address","[']",False)
                result = i_run_sql("reset persist group_replication_group_seeds","[']", False)
                try:
                    result = i_run_sql("RESTART","[']", False)
                except:
                    print("\n\033[1mERROR:\033[0m Restart server failed (mysqld is not managed by supervisor process)\n")
            print('test 7')
            shell.set_session(local_instance)
            for node in i_get_other_node():
                try:
                    if shell.parse_uri(node)["port"] > 10000:
                        n = node[:-1]
                    else:
                        n = shell.parse_uri(node)["host"] + ":" + str(shell.parse_uri(node)["port"] - 10)

                    if n != report_host[0].strip("report_host', '") + ":" + report_port[0].strip("report_port', '"):
                        print("\n\033[96m*****\033[0m\n")
                        print("\033[1mINFO:\033[0m \033[96mResync Group Replication Members on " + n + "\033[0m")
                        y=shell.open_session(clusterAdmin + "@" + n, clusterAdminPassword)
                        shell.set_session(y)
                        syncLocalMembers()
                except:
                    print("\033[1mINFO:\033[0m Unable to connect to '" + n + "', SKIPPED\n!")

            print("\n\033[96m*****\033[0m\n")
            print("\033[1mINFO:\033[0m Resync \033[96mLocal\033[0m Group Replication Members")
            shell.set_session(local_instance)
            syncLocalMembers()
        except:
            print("\n\033[1mERROR:\033[0m Unable to connect to " + connectionStr)
            shell.set_session(local_instance)
    else:
        print("\n\033[1mINFO:\033[0m Instance removal is cancelled \n")

@plugin_function("group_replication.dissolve")
def dissolve():
  """
  Dissolve group replication
  This function removes existing Group Replication
  """
  global clusterAdminPassword
  global autoFlipProcess

  import time

  if not i_checkInstanceConfiguration():
    print('\n\033[1mERROR:\033[0m Instance is not a Group Replciation or User is not a cluster admin\n')
    return
  try:
    x = shell.get_session()
    clusterAdmin, foo, hostname, port = i_sess_identity("current")
    if not clusterAdminPassword:
        clusterAdminPassword = shell.prompt("Please provide the password for \033[96m'" + clusterAdmin + "'\033[0m : ", {"type":"password"})
    for node in i_get_other_node():
       try:
           if shell.parse_uri(node)["port"] > 10000:
               n = node[:-1]
           else:
               n = shell.parse_uri(node)["host"] + ":" + str(shell.parse_uri(node)["port"] - 10)
           removeInstance(clusterAdmin + "@" + n)
       except:
           print("\033[1mINFO: \033[0m Unable to connect to '" + node + "', SKIPPED!")
    shell.set_session(x)
    print("We need to RESTART MySQL ...")
    removeInstance(str(x).strip("<ClassicSession:>"))
    print("Waiting for MySQL restart ...")
    restart_status = False
    while not restart_status:
        time.sleep(10)
        shell.reconnect()    
        connect_status = str(shell.status())
        if connect_status.find('Not Connected') == -1:
            restart_status = True
  except:
     print("\033[1mERROR: \033[0m This instance is not part of a group replication")

@plugin_function("group_replication.create")
def create():
    """
    Create MySQL Group Replication
    This function creates a Group Replication environment
    """

    global convert_to_gr
    global autoFlipProcess

    if not convert_to_gr:
        try:
            result = i_run_sql("show variables like 'group_replication_group_name'","[']",False)
            z = result[0].strip("group_replication_group_name', '")
            if z != "":
                print("\n\033[1mINFO:\033[0m Group Replication Group Name : " + z)
                print("\n\033[1mERROR:\033[0m Unable to create on existing Group Replication \033[0m\n")
                return
        except:
            print("\n")

    if not i_checkInstanceConfiguration():
        return
    else:
        if not autoFlipProcess:
            print("\n\033[1mConfiguring Group Replication ... \033[0m\n")
            print("Please ensure you started this instance with skip-slave-start")
            x=shell.prompt("Do you want to continue (Y/n): ",{"defaultValue":"Y"}).upper()
            if x != "Y":
                print("\n\033[1mGroup Replication Creation Aborted !\033[0m")
                return

    if _check_report_host():
        clusterAdmin, foo, hostname, port = i_sess_identity("current")
        gr_seed = "{}:{}".format(hostname, int(port) + 10)
        try:
            result = i_create_or_add("CREATE",gr_seed, i_define_gr_name(), gr_seed)
            print("\n\033[1mGroup Replication Creation is successful ! \033[0m\n")
            return status()
        except:
            print("\n\033[1mERROR:\033[0m Group Replication Creation Aborted ! \n")
            return
    else:
        print("\n\033[1mERROR:\033[0m Failed in checking report host. Group Replication Creation Aborted !\n")

@plugin_function("group_replication.addInstance")
def addInstance(connectionStr):
    """
    Add instance to group replication
    This function adds an instance to an existing Group Replication
    Args:
        connectionStr (string): uri clusterAdmin:clusterAdminPassword@hostname:port
    """
    global convert_to_gr
    global clusterAdminPassword
    global autoFlipProcess
    global autoCloneProcess

    x = shell.get_session()

    try:
        clusterAdminPassword=shell.parse_uri(connectionStr)['password']
        y = shell.open_session(connectionStr)
        shell.set_session(y)
    except:
        try:
            if not clusterAdminPassword:
                clusterAdminPassword = shell.prompt("Please provide the password for '{}' : ".format(connectionStr),{'type': 'password'})
            y = shell.open_session(connectionStr, clusterAdminPassword)
            shell.set_session(y)
        except:
            print("\n\033[1mERROR:\033[0m Unable to connect to '\033[1m" + connectionStr + "\033[0m'\n")
            return

    if not convert_to_gr:
        try:
            result = i_run_sql("show variables like 'group_replication_group_name'","[']",False)
            z = result[0].strip("group_replication_group_name', '")
            if z != "":
                print("\n\033[1mINFO:\033[0m Group Replication Group Name : " + z)
                print("\n\033[1mERROR:\033[0m Unable to add instance on existing Group Replication \033[0m\n")
                shell.set_session(x)
                return
        except:
            print("\n")

    if not i_checkInstanceConfiguration():
        print("\n\033[1mERROR:\033[0m Group Replication Adding Instance Aborted ! \033[0m\n")
        shell.set_session(x)
        return

    shell.set_session(x)
    print("\n\033[1mConfiguring Group Replication ... \033[0m\n")
    print("Please ensure you started this instance with skip-slave-start")

    if not autoFlipProcess:
        x=shell.prompt("Do you want to continue (Y/n): ",{"defaultValue":"Y"}).upper()
        if x != "Y":
            print("\n\033[1mGroup Replication Adding Instance Aborted !\033[0m")
            return

    clusterAdmin, foo, hostname, port = i_sess_identity("current")

    print("A new instance will be added to the Group Replication. Depending on the amount of data on the group this might take from a few seconds to several hours. \n")

    if not autoFlipProcess:
        clone_sts = shell.prompt("Please select a recovery method [C]lone/[I]ncremental recovery/[A]bort (default Clone): ").upper()
    else:
        if not autoCloneProcess:
            clone_sts = "I"
        else:
            clone_sts = "C"

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

        x=shell.get_session()
        shell.set_session(x)
        i_set_all_grseed_replicas(old_gr_seed, new_gr_seed, clusterAdmin, clusterAdminPassword)
        i_create_or_add(clone_sts,add_gr_node,i_get_gr_name(), new_gr_seed)
    return status()

@plugin_function("group_replication.syncLocalMembers")
def syncLocalMembers():
    """
    Synchronize group_replication_group_seeds variable as the plugin metadata
    This function makes group_replication_group_seeds consistent with performance_schema.replication_group_members 
    """
    result = i_run_sql("select concat(member_host,':',member_port) from performance_schema.replication_group_members where channel_name='group_replication_applier'", "[']", False)
    if len(result) != 0:
        group_replication_group_members=''
        for node in result:
            group_replication_group_members=group_replication_group_members + ',' + shell.parse_uri(node)["host"] + ':' + str(shell.parse_uri(node)["port"] + 10)
        result = i_run_sql("set persist group_replication_group_seeds='" + group_replication_group_members[1:] + "'","", False)
        print("\n\033[1mINFO:\033[0m Group Replication members is synched")
    return status()

@plugin_function("group_replication.convertToIC")
def convertToIC(clusterName):
    """
    Convert From Group Replication to InnoDB Cluster
    This function converts a Group Replication environment to MySQL InnoDB Cluster
    Args:
        clusterName (string): Any name for your InnoDB Cluster
    """
    x=shell.get_session()
    if i_check_local_role() != "PRIMARY":
        current_primary = i_run_sql("select concat(member_host,':',member_port) from performance_schema.replication_group_members where channel_name='group_replication_applier' and member_role='PRIMARY'","[']",False)
        cp_user=shell.prompt('Please provide user to connect to current PRIMARY node (' + current_primary[0] + '): ')
        # pp_user=shell.prompt('Please provide password to connect to current PRIMARY node (' + current_primary[0] + '): ', {"type":"password"})
        try:
            c=shell.open_session(cp_user + '@' + current_primary[0])
            shell.set_session(c)
        except:
            shell.set_session(x)
            print('ERROR: unable to connect to PRIMARY server !')

    msg_output = "0"
    i_stop_other_replicas()
    i_drop_ic_metadata()
    dba.create_cluster(clusterName, {"adoptFromGR":True})
    msg_output = "Successful conversion from Group Replication to InnoDB Cluster"
    shell.set_session(x)
    return msg_output

@plugin_function("group_replication.adoptFromIC")
def adoptFromIC():
    """
    Convert From InnoDB Cluster to native Group Replication
    This function converts a MySQL InnoDB Cluster to a native Group Replication environment
    """
    global convert_to_gr
    global autoFlipProcess
    global clusterAdminPassword

    try:
        dba.get_cluster()
    except:
        print('\n\033[1mINFO:\033[0m Failed to convert, this is not an InnoDB Cluster\n')
        return

    if not autoFlipProcess:
        p = shell.prompt("\nAre you sure to convert this cluster into Group Replication ? (y/N) ",{'defaultValue': "N"}).upper()
        if p == "N":
            print("\033[1mINFO: \033[0m Operation is aborted !\n")
            return

    c_primary=""
    x=shell.get_session()
    if i_check_local_role() != "PRIMARY":
        current_primary = i_run_sql("select concat(member_host,':',member_port) from performance_schema.replication_group_members where channel_name='group_replication_applier' and member_role='PRIMARY'","[']",False)
        cp_user=shell.prompt('Please provide user to connect to current PRIMARY node (' + current_primary[0] + '): ')
        # pp_user=shell.prompt('Please provide password to connect to current PRIMARY node (' + current_primary[0] + '): ', {"type":"password"})
        try:
            c=shell.open_session(cp_user + '@' + current_primary[0])
            c_primary=current_primary[0]
            shell.set_session(c)
        except:
            shell.set_session(x)
            print('ERROR: unable to connect to PRIMARY server !')

    clusterAdmin, foo, hostname, port = i_sess_identity("current")

    host_list = i_get_other_node()

    dba.get_cluster().dissolve({"interactive":False})
    convert_to_gr = True
    print("\n\033[96m*****\033[0m\n")
    print("\033[96mINFO: Create Group Replication on " + c_primary + "\033[0m")
    create()
    convert_to_gr = False
    if len(host_list) != 0:
        for secNode in host_list:
            print("\n\n\033[96m*****\033[1m\n")
            if shell.parse_uri(secNode)["port"] > 10000:
                try:
                    print('\033[1mINFO:\033[0m \033[96mAdding instance ' + secNode[:-1] +"\033[0m")
                    convert_to_gr = True
                    addInstance(clusterAdmin + "@" + secNode[:-1])
                    convert_to_gr = False
                except:
                    print("\033[1mINFO: \033[0m Unable to convert '" + node + "', SKIPPED!")
            else:
                try:
                    print('\033[1mINFO:\033[0m \033[96mAdding instance ' + shell.parse_uri(secNode)["host"] + ":" + str(shell.parse_uri(secNode)["port"] - 10) + "\033[0m")
                    convert_to_gr=True
                    addInstance(clusterAdmin + "@" + shell.parse_uri(secNode)["host"] + ":" + str(shell.parse_uri(secNode)["port"] - 10))
                    convert_to_gr=False
                except:
                    print("\033[1mINFO: \033[0m Unable to convert '" + node + "', SKIPPED!")
    i_drop_ic_metadata()
    msg_output = "Successful conversion from InnoDB Cluster to Group Replication\n"
    shell.set_session(x)
    return msg_output

@plugin_function("group_replication.rebootGRFromCompleteOutage")
def rebootGRFromCompleteOutage():
   """
   Startup Group Replication
   This function starts Group Replication
   """
   if not i_checkInstanceConfiguration():
       return

   clusterAdmin, clusterAdminPassword, hostname, port = i_sess_identity("current")
   print("\nUsername for distributed recovery is " + clusterAdmin)
   clusterAdminPassword = shell.prompt('Please provide password for ' + clusterAdmin + ': ',{"type":"password"})
   x=shell.get_session()

   try:
       y=shell.open_session(clusterAdmin + "@" + hostname + ":" + port, clusterAdminPassword)
       shell.set_session(y)
   except:
       print("\n\033[1mERROR:\033[0m Password mismatch \033[0m\n")
       return

   local_gtid = i_get_gtid()
   process_sts = "Y"
   for node in i_get_other_node():
       try:
           if shell.parse_uri(node)["port"] > 10000:
               n = node[:-1]
           else:
               n = shell.parse_uri(node)["host"] + ":" + str(shell.parse_uri(node)["port"] - 10)
           y=shell.open_session(clusterAdmin + "@" + n, clusterAdminPassword)
           shell.set_session(y)
           remote_gtid = i_get_gtid()
           if i_comp_gtid(local_gtid, remote_gtid) != "1":
               process_sts = "N"
       except:
           print("\033[1mINFO: \033[0m Unable to connect to '" + node + "', SKIPPED!")

   if process_sts == "Y":
       shell.set_session(x)
       print("This node is suitable to start Group Replication")
       print("Process may take a while ...")
       i_start_gr_all(clusterAdmin, clusterAdminPassword)
       print("Reboot Group Replication process is completed")
   else:
       print("Node was not a PRIMARY, try another node")
   return status()

@plugin_function("group_replication.checkInstanceConfiguration")
def checkInstanceConfiguration():
    """
    Check instance readiness to be a Group Replication member
    This function checks if local instance can be added into a Group Replication using this plugin
    """

    i = i_checkInstanceConfiguration()
    if not i:
        print("\n\033[1mERROR:\033[0m Instance is not ready for Group Replication\n")
    else:
        print("\n\033[1mINFO:\033[0m Instance is ready for Group Replication\n")


#####################################################
#
#  MULTI CLUSTERS FUNCTIONS
#  INNODB CLUSTER REPLICATION TO GROUP REPLICATION
#


@plugin_function("group_replication.startMultiClusterChannel")
def startMultiClusterChannel(channel_name, session=None):
    """
    Start Replication Channel
    This function starts the replication channel
    Args:
        channel_name (string): The replication channel's name.
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.
    """
    global remote_user
    global remote_password

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    try:
        result = i_run_sql("start replica for channel '" + channel_name + "'", "", False)
        print('\n\033[1mINFO:\033[0m Replication channel is started \n')
        print('Starting replication monitoring event ...')
        time.sleep(10)
        try:
            if (not remote_user) or (not remote_password):
                r_user = i_run_sql("select repl_user from mysql_gr_replication_metadata.channel","[']",False)
                r_password = shell.prompt("Please provide the password for '" + remote_user[0] + "' : ", {'type':'password'})
                remote_user = r_user[0]
                remote_password = r_password[0]
            h=shell.parse_uri(shell.get_session().get_uri())['host']
            p=shell.parse_uri(shell.get_session().get_uri())['port']
            local_repl=shell.open_session(remote_user + '@' + h + ':' + str(p), remote_password)
            shell.set_session(local_repl)

            result = i_run_sql("create event if not exists mysql_gr_replication_metadata." + channel_name + " ON SCHEDULE every 2 second do start replica for channel '" + channel_name + "';", "", False)
            shell.get_session().run_sql("SET GLOBAL event_scheduler = OFF;")
            shell.get_session().run_sql("alter event mysql_gr_replication_metadata." + channel_name + " enable;")
            shell.get_session().run_sql("SET GLOBAL event_scheduler = ON;")

            shell.set_session(session)
            print('\033[96mStarted.\033[0m \n')
        except:
            print('\033[1mWARNING:\033[0m \033[96mmysql_gr_replication_metadata\033[0m does not exist - ignored !\n')
    except:
        print('\033[1mERROR:\033[0m unable to start replication for channel ' + channel_name + "\n")

@plugin_function("group_replication.stopMultiClusterChannel")
def stopMultiClusterChannel(channel_name, session=None):
    """
    Stop Replication Channel
    This function stops the replication channel
    Args:
        channel_name (string): The replication channel's name.
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.
    """
    global remote_user
    global remote_password

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    try:
        try:
            if (not remote_user) or (not remote_password):
                r_user = i_run_sql("select repl_user from mysql_gr_replication_metadata.channel","[']",False)
                r_password = shell.prompt("Please provide the password for '" + r_user[0] + "' : ", {'type':'password'})
                remote_user = r_user[0]
                remote_password = r_user[0]
            h=shell.parse_uri(shell.get_session().get_uri())['host']
            p=shell.parse_uri(shell.get_session().get_uri())['port']
            local_repl=shell.open_session(remote_user + '@' + h + ':' + str(p), remote_password)
            shell.set_session(local_repl)
            shell.get_session().run_sql("alter event mysql_gr_replication_metadata." + channel_name + " disable;")
        except:
            print('\n\033[1mWARNING:\033[0m \033[96mmysql_gr_replication_metadata\033[0m schema does not exist - ignored!')
        shell.set_session(session)
        shell.get_session().run_sql("stop replica for channel '" + channel_name + "'")
        print('\n\033[1mINFO:\033[0m replication is stopped \n')
    except:
        print('\033[1mERROR:\033[0m Unable to stop replication channel ' + channel_name + "\n")

@plugin_function("group_replication.setFailoverOnChannel")
def setFailoverOnChannel(channel_name):
    """
    Set Replication Asynchronous Failover (for MySQL version >= 8.0.22)
    This function implements register all remote InnoDB Cluster nodes into mysql.replication_asynchronous_connection_failover for replication channel High Availability
    Args:
        channel_name (string): The replication channel's name.
    """
    global remote_password

    if i_check_local_role() != 'PRIMARY':
        print('\n\033[1mERROR:\033[0m This function has to be executed from PRIMARY node \n')
        return

    try:
        result = i_run_sql("select repl_user from mysql_gr_replication_metadata.channel;","[']",False)
        repl_user=result[0]
    except:
        print("\n\033[1mERROR:\033[0m Multi Cluster replication user is not set \n")
        return

    try:
        result = i_run_sql("select host from mysql.slave_master_info where channel_name='" + channel_name + "'", "[']", False)
        router_host=result[0]
    except:
        print("\n\033[1mERROR:\033[0m could not get master info from mysql.slave_master_info\n")
        return

    try:
        result = i_run_sql("select port from mysql.slave_master_info where channel_name='" + channel_name + "'", "[']", False)
        router_port=result[0]
    except:
        print("\n\033[1mERROR:\033[0m could not get master info from mysql.slave_master_info \n")
        return
    print("\n\033[96mTest connection to " + repl_user + "@" + router_host + ":" + str(router_port) + "\033[0m\n")
    
    if not remote_password:
        repl_password = shell.prompt("Please confirm password for '" + repl_user + "' ", {"type":"password"})
    else:
        repl_password = remote_password

    try:
        x=shell.get_session()
        y=shell.open_session(repl_user + "@" + router_host + ":" + str(router_port), repl_password)
        shell.set_session(y)
        result = i_run_sql("select concat(member_host,':',member_port) from performance_schema.replication_group_members where channel_name='group_replication_applier'","[']",False)
        # shell.set_session(x)
        try:
            r = i_run_sql("select concat(host,':',port)  from mysql.replication_asynchronous_connection_failover where channel_name='" + channel_name + "'", "[']",False)
            if len(r) > 0:
                for row in range(len(r)):
                    _host = shell.parse_uri(r[row])["host"]
                    _port = shell.parse_uri(r[row])["port"]
                    
                    s = i_run_sql("select asynchronous_connection_failover_delete_source('" + channel_name + "', '" + _host + "', " + str(_port) + ", '')","",False)

            if len(result) > 0:
                for row in range(len(result)):
                    _host = shell.parse_uri(result[row])["host"]
                    _port = shell.parse_uri(result[row])["port"]
                    r = i_run_sql("select asynchronous_connection_failover_add_source('" + channel_name +"', '" + _host + "', " + str(_port) +", '', 50)","[']",False)
                shell.set_session(x)
                stopMultiClusterChannel(channel_name)
                rs = i_run_sql("change master to master_host='" + router_host + "', master_port=" + str(router_port) + ", master_user='" + repl_user + "', master_password='" + repl_password + "', master_ssl=1, master_auto_position=1, source_connection_auto_failover=1, master_connect_retry=3, master_retry_count=3, get_master_public_key=1 for channel '" + channel_name + "'", "", False)
                startMultiClusterChannel(channel_name)
            return shell.get_session().run_sql("select * from mysql.replication_asynchronous_connection_failover")
        except:
            print("\033[1mERROR:\033[0m unable to setup async replication failover \n")
            return   
    except:
        print("\033[1mERROR:\033[0m unable to establish connection to " + router_host + ":" + str(router_port) + "\n")
        shell.set_session(x)
        return


@plugin_function("group_replication.editMultiClusterChannel")
def editMultiClusterChannel(channel_name, endpoint_host, endpoint_port_number, session=None):
    """
    Edit Replication Channel's configuration
    This function is used to edit / change existing channel configuration
    Args:
        channel_name (string): Any name for your InnoDB Cluster
        endpoint_host (string): InnoDB Cluster host or router host
        endpoint_port_number (integer): InnoDB Cluster node port or router port
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

    stopMultiClusterChannel(channel_name)
    setMultiClusterChannel(channel_name, endpoint_host, endpoint_port_number)    
    startMultiClusterChannel(channel_name)
    return showChannel()

@plugin_function("group_replication.setMultiClusterChannel")
def setMultiClusterChannel(channel_name, router_host, router_port_number, session=None):
    """
    Configure Replication Channel 
    This function configures replication channel from InnoDB Cluster to Group Replication
    Args:
        channel_name (string): Any name for your InnoDB Cluster
        router_host (string): InnoDB Cluster router host or InnoDB Cluster node
        router_port_number (integer): InnoDB Cluster router port or InnoDB Cluster node port
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.
    """
    import time

    global remote_user
    global remote_password
    global clusterAdminPassword
    global autoFlipProcess

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    router_port = str(router_port_number)
    clusterAdmin, foo, hostname, port = i_sess_identity("current")
    
    if not remote_user:
        remote_user = shell.prompt('\n\033[96mPlease provide user to connect to remote database : \033[0m')
        remote_password = shell.prompt("Please provide password for '" + remote_user + "': ",{"type":"password"})
    
    # Create group replication's replication metadata schema on other cluster
    x=shell.get_session()
    try:
        y=shell.open_session(remote_user + "@" + router_host + ":" + router_port, remote_password)
        shell.set_session(y)
        result = i_run_sql('create database if not exists mysql_gr_replication_metadata',"[']",False)
        result = i_run_sql('create table if not exists mysql_gr_replication_metadata.channel (channel_name varchar(10) primary key, repl_user varchar(10))',"", False)
        result = i_run_sql('delete from mysql_gr_replication_metadata.channel',"",False)
        result = i_run_sql("insert into mysql_gr_replication_metadata.channel values ('" + channel_name + "','" + remote_user + "');","",False)
        result = i_run_sql("grant all privileges on mysql_gr_replication_metadata.* to " + clusterAdmin, "", False)
    except:
        print('\n\033[1mERROR:\033[0m Remote database does not exist or this user does not have create database privileges')
        print("Mandatory Requirement for Replication between InnoDB Cluster and Group Replication :")
        print("\033[96mboth clusters have to use same cluster user\033[0m\n") 
        shell.set_session(x)
        return

    shell.set_session(x)
    print('\033[1mINFO:\033[0m \033[96mmysql_gr_replication_metadata\033[0m schema is installed on remote database\n')
    print('\033[1mINFO:\033[0m configuring replication channel ...')
    
    if not clusterAdminPassword:
        clusterAdminPassword = shell.prompt("Please provide the password for \033[96m'" + clusterAdmin + "'\033[0m : ", {"type":"password"}) 
   
    for node in i_get_other_node():
        print("\n\033[96mINFO:\033[0m Configuring replication channel on '" + shell.parse_uri(node)["host"] + ":" + str(shell.parse_uri(node)["port"] - 10))
        y=shell.open_session(clusterAdmin + "@" + shell.parse_uri(node)["host"] + ":" + str(shell.parse_uri(node)["port"] - 10), clusterAdminPassword)
        shell.set_session(y)
        result = i_run_sql("change master to master_host='" + router_host + "', master_port=" + router_port + ", master_user='" + remote_user + "', master_password='" + remote_password + "', master_ssl=1, master_auto_position=1, get_master_public_key=1 for channel '" + channel_name + "'", "", False)
    shell.set_session(x)
    print("\n\033[96mINFO:\033[0m Configuring replication channel on " + hostname + ":" + port)
    result = i_run_sql("change master to master_host='" + router_host + "', master_port=" + router_port + ", master_user='" + remote_user + "', master_password='" + remote_password + "', master_ssl=1, master_auto_position=1, get_master_public_key=1 for channel '" + channel_name + "'", "", False)
    print("\n\033[1mINFO:\033[0m Starting replication from " + router_host + ":" + router_port + " ...")
    
    try:
       result = i_run_sql("start replica for channel '" + channel_name + "'", "", False)
    except:
       result = i_run_sql("stop replica for channel '" + channel_name + "'", "", False)
       result = i_run_sql("reset replica for channel '" + channel_name + "'", "", False)
       result = i_run_sql("start replica for channel '" + channel_name + "'", "", False)

    print('\n\033[1mINFO:\033[0m Waiting for mysql_gr_replication_metadata being replicated ...')
    is_schema_exist=0
    while is_schema_exist != 1:
        result = i_run_sql("select schema_name from information_schema.schemata where schema_name='mysql_gr_replication_metadata';","",False)
        if len(result) != 0:
            is_schema_exist=1
    
    time.sleep(10)
    print('\n\033[1mINFO:\033[0m mysql_gr_replication_metadata schema is replicated successfuly. An event will be created')
  
    try:
        h=shell.parse_uri(shell.get_session().get_uri())['host']
        p=shell.parse_uri(shell.get_session().get_uri())['port']
        local_repl=shell.open_session(remote_user + '@' + h + ':' + str(p), remote_password)
        shell.set_session(local_repl)
        result = i_run_sql("create event if not exists mysql_gr_replication_metadata." + channel_name + " ON SCHEDULE every 2 second do start replica for channel '" + channel_name + "';", "", False)
        print('\033[1mINFO:\033[0m An event is created to monitor channel ' + channel_name)
        shell.set_session(x)
    except:
        print('\033[1mWARNING:\033[0m mysql_gr_replication_metadata schema does not exist.')
    return print("\n\033[1mAll nodes have this replication channel\033[0m\n")

@plugin_function("group_replication.setMultiClusterReplUser")
def setMultiClusterReplUser(repl_user):
    """
    Setup database user on InnoDB Cluster to be used for replication to Group Replication
    This function creates user, assign privileges required for replication. Login to PRIMARY node of InnoDB Cluster using 'root' user for executing this function.
    Args:
        repl_user (string): new database user name
    """
    try:
      dba.get_cluster()
      user_password = shell.prompt("\nPlease provide password for '" + repl_user + "' : ", {"type":"password"})
      confirm_password = shell.prompt("Please confirm password for '" + repl_user + "' ", {"type":"password"})
      if user_password != confirm_password:
          print("\n\033[1mERROR:\033[0m Password mismatch ! \n")
          return

      result = i_run_sql("create user if not exists " + repl_user + " identified by '" + user_password + "'", "" , False)
      result = i_run_sql("grant replication slave on *.* to " + repl_user + "@'%';", "", False)
      result = i_run_sql("grant all privileges on *.* to " + repl_user + "@'%' with grant option;", "", False)
      print('\n\033[1mINFO:\033[0m User ' + repl_user + ' is created with all required privileges for Group Replication to connect \n')
    except:
        print('\n\033[1mERROR:\033[0m Running this function on a Group Replication is prohibited')
        print('\033[1mERROR:\033[0m Check if your connection ' +  str(shell.get_session()) + ' has sufficient privileges to configure this user')
        print('\n\033[96mINFO:\033[0m Try to run this function using root user \n')
     

@plugin_function("group_replication.flipClusterRoles")
def flipClusterRoles(cluster_name):
    """
    Flip Cluster Role is to be used for flipping InnoDB Cluster to Group Replication and vice versa
    This function is used for DR switchover
    Args:
        cluster_name (string): InnoDB Cluster name
    """
    global clusterAdminPassword
    global recovery_user
    global recovery_password
    global remote_user
    global remote_password
    global autoFlipProcess

    if not i_checkInstanceConfiguration():
        print('\n\033[1mERROR:\033[0m Instance is not a Group Replication or User is not a cluster admin\n')
        return

    try:
        dba.get_cluster()
        v_continue = 0
    except:
        v_continue = 1

    if v_continue == 0:
        print('\n\033[1mERROR:\033[0m Flip cluster roles has to be executed on the Group Replication\n')
        return
    
    if i_check_local_role() != 'PRIMARY':
        print('\n\033[1mERROR:\033[0m Flip Cluster Roles has to be executed from PRIMARY node \n')
        return

    print("\n\033[92m PREPARATION: \033[0m\n")
    autoFlipProcess = True
    clusterAdmin, foo, hostname, port = i_sess_identity("current")
    clusterAdminPassword = shell.prompt("Please provide the password for '" + clusterAdmin + "' : ", {'type':'password'})
    recovery_user = shell.prompt("Which user do you want to use for distributed recovery ? : ")
    recovery_password = shell.prompt("Please provide the password for '" + recovery_user + "' : ", {'type':'password'})
    
    try:
        result = i_run_sql("select repl_user from mysql_gr_replication_metadata.channel;","[']",False)
        repl_user=result[0]
    except:
        print("\n\033[1mERROR:\033[0m Multi Cluster replication user is not set \n")
        return

    remote_user = repl_user
    remote_password = shell.prompt("Please provide the password for '" + remote_user + "' : ", {'type':'password'})

    # set all identifies
    try:
        result = i_run_sql("select channel_name from mysql_gr_replication_metadata.channel;","[']",False)
        channel_name=result[0]
    except:
        print("\n\033[1mERROR:\033[0m replication from InnoDB Cluster is not set \n")
        return 

    try:
        result = i_run_sql("select host from mysql.slave_master_info where channel_name='" + channel_name + "'", "[']", False)
        router_host=result[0]
    except:
        print("\n\033[1mERROR:\033[0m could not get master info from mysql.slave_master_info\n")
        return

    try:
        result = i_run_sql("select port from mysql.slave_master_info where channel_name='" + channel_name + "'", "[']", False)
        router_port=result[0]
    except:
        print("\n\033[1mERROR:\033[0m could not get master info from mysql.slave_master_info \n")
        return

    try:
        x=shell.get_session()
        y=shell.open_session(repl_user + "@" + router_host + ":" + str(router_port), remote_password)
        shell.set_session(y)
        result = i_run_sql("select member_host from performance_schema.replication_group_members where channel_name='group_replication_applier' and member_role='PRIMARY'","[']",False)
        remote_primary_host=result[0]
        result = i_run_sql("select member_port from performance_schema.replication_group_members where channel_name='group_replication_applier' and member_role='PRIMARY'","[']",False)
        remote_primary_port=result[0]
        shell.set_session(x)
        print('\n\033[1mINFO:\033[0m remote primary host and port is ' + remote_primary_host + ':' + str(remote_primary_port) + "\n")
    except:
        print("\033[1mERROR:\033[0m unable to establish connection to '" + remote_primary_host + ":" + str(remote_primary_port) + "\n")
        shell.set_session(x)
        return

    print("\n\n\033[92m********** STEP 1 : Set Replication directly to PRIMARY node **********\033[0m")
    print("Purpose: to bypass any MySQL Router by connecting directly to InnoDB Cluster primary node")
    try:
        editMultiClusterChannel(channel_name,remote_primary_host,remote_primary_port)
    except:
        print("\n\033[1mABORT:\033[0m Unable to switch replication channel to remote PRIMARY node \n")
        return

    print("\033[96mINFO:\033[0m End of Step 1 \n\n")
    
    print("\n\033[92m********** STEP 2 : Convert remote cluster to Group Replication **********\033[0m\n")
    print('Purpose: To convert remote InnoDB Cluster into Group Replication')

    try:
        y=shell.open_session(clusterAdmin + "@" + remote_primary_host + ":" + str(remote_primary_port), clusterAdminPassword)
        shell.set_session(y)
    except:
        print('\033[1mERROR:\033[0m unable to connect to remote primary host')
        print("We have not convert anything. Just make sure InnoDB Cluster is reachable and execute this command again")
        print("If flip Clusters is cancelled, just run editMultiClusterChannel() to connect to router \n")
        shell.set_session(x)
        return
    
    try:
        adoptFromIC()
    except:
        print('\n\033[1mABORT:\033[0m unable to convert remote cluster to Group Replication\n')
        return

    try:
        shell.set_session(x)
        stopMultiClusterChannel(channel_name)
    except:
        print('\n\033[1mABORT:\033[0m unable to stop local cluster replication channel \n')
        return

    print("\n\n\033[92m********** STEP 3 : Set replication from remote cluster to this node *********\033[0m")
    print("Purpose: to flip the Multi Cluster Replication Channel from remote cluster to this cluster")
    try:
        shell.set_session(y)
        setMultiClusterChannel(channel_name,hostname,port)
    except:
        print('\n\033[1mABORT:\033[0m unable to set replication channel on remote cluster \n')
        return

    print("\n\n\033[92m********** STEP 4 : Change local cluster to InnoDB Cluster *********\033[0m")
    try:
        shell.set_session(x)
        convertToIC(cluster_name)
    except:
        print('\n\033[1mABORT:\033[0m unable to convert local cluster to InnoDB Cluster \n')
        return

    print("\n\n\033[92m********* STEP 5 : Additional Manual Steps *********\033[0m")
    
    result = i_run_sql("select count(1) from mysql.replication_asynchronous_connection_failover order by channel_name, host, port","[']", False)
    if int(result[0]) > 0:
        print("\033[1mINFO:\033[0m Replication Asynchronous Failover is IMPLEMENTED")
        shell.set_session(y)
        setFailoverOnChannel(channel_name)
        shell.set_session(x)
    else:
        print("\033[1mWARNING:\033[0m Replication Asynchronous Failover is NOT IMPLEMENTED")
        print("You must configure \033[96mrouter\033[0m for a multi cluster replication")
    autoFlipProcess = None
    clusterAdminPassword = None
    recovery_user = None
    recovery_password = None
    remote_user = None
    remote_password = None

@plugin_function("group_replication.removeFailoverChannel")
def removeFailoverChannel(channel_name):
    """
    Remove Failover Channel is used to convert replication to InnoDB Cluster
    This function is used to migrate from NON-ROUTER replication to ROUTER based replication for replication connection HA.
    Args:
        channel_name (string): Replication Channel Name
    """
    global clusterAdminPassword

    if not i_checkInstanceConfiguration():
        print('\n\033[1mERROR:\033[0m Instance is not a Group Replication or User is not a cluster admin\n')
        return

    try:
        dba.get_cluster()
        v_continue = 0
    except:
        v_continue = 1

    if v_continue == 0:
        print('\n\033[1mERROR:\033[0m Removing Failover Channel has to be executed on the Group Replication\n')
        return

    if i_check_local_role() != 'PRIMARY':
        print('\n\033[1mERROR:\033[0m Removing Failover Channel has to be executed from PRIMARY node \n')
        return

    try:
        result = i_run_sql("select host from mysql.slave_master_info where channel_name='" + channel_name + "'", "[']", False)
        router_host=result[0]
    except:
        print("\n\033[1mERROR:\033[0m could not get master info from mysql.slave_master_info\n")
        return

    try:
        result = i_run_sql("select port from mysql.slave_master_info where channel_name='" + channel_name + "'", "[']", False)
        router_port=result[0]
    except:
        print("\n\033[1mERROR:\033[0m could not get master info from mysql.slave_master_info \n")
        return

    clusterAdmin, foo, hostname, port = i_sess_identity("current")
    if not clusterAdminPassword:
        clusterAdminPassword = shell.prompt("Please provide the password for '" + clusterAdmin + "' : ", {'type':'password'})

    try:
        r = i_run_sql("select concat(host,':',port)  from mysql.replication_asynchronous_connection_failover where channel_name='" + channel_name + "'", "[']",False)
    except:
        print("\n\033[1mERROR:\033[0m This version does not support Replication Asynchronous Connection Failover\n")
    
    x = shell.get_session()

    if len(r) > 0:
        try:
            y=shell.open_session(clusterAdmin + "@" + router_host + ":" + str(router_port), clusterAdminPassword)
            shell.set_session(y)
            result = i_run_sql("select member_host from performance_schema.replication_group_members where channel_name='group_replication_applier' and member_role='PRIMARY'","[']",False)
            remote_primary_host=result[0]
            result = i_run_sql("select member_port from performance_schema.replication_group_members where channel_name='group_replication_applier' and member_role='PRIMARY'","[']",False)
            remote_primary_port=result[0]
            y=shell.open_session(clusterAdmin + "@" + remote_primary_host + ":" + str(remote_primary_port), clusterAdminPassword)
            shell.set_session(y)
        except:
            print("\033[1mERROR:\033[0m unable to establish connection to '" + remote_primary_host + ":" + str(remote_primary_port) + "\n")
            shell.set_session(x)
            return
        for row in range(len(r)):
            _host = shell.parse_uri(r[row])["host"]
            _port = shell.parse_uri(r[row])["port"]

            s = i_run_sql("select asynchronous_connection_failover_delete_source('" + channel_name + "', '" + _host + "', " + str(_port) + ", '')","",False)
        shell.set_session(x)
        print("\n\033[1mINFO:\033[0m Replication Asynchronous Connection Failover is DISABLED\n")
    else:
        print("\n\033[1mINFO:\033[0m Replication Asynchronous Connection Failover is NOT IMPLEMENTED\n")

@plugin_function("group_replication.autoCloneICtoGR")
def autoCloneICtoGR():
    """
    Auto Cloning from InnoDB Cluster to Group Replication
    This function is used to Clone PRIMARY Node of InnoDB Cluster to all Nodes of Group Replication
    USING SINGLE API COMMAND
    """
    import time

    global clusterAdminPassword
    global recovery_user
    global recovery_password
    global autoFlipProcess
    global autoCloneProcess

    if not i_checkInstanceConfiguration():
        print('\n\033[1mERROR:\033[0m Instance is not a Group Replication or User is not a cluster admin\n')
        return

    try:
        dba.get_cluster()
        v_continue = 0
    except:
        v_continue = 1

    if v_continue == 0:
        print('\n\033[1mERROR:\033[0m DB Cloning has to be executed on the Group Replication\n')
        return

    if i_check_local_role() != 'PRIMARY':
        print('\n\033[1mERROR:\033[0m DB Cloning has to be executed from PRIMARY node \n')
        return

    print("\n\033[92m PREPARATION: \033[0m\n")
    clusterAdmin, foo, hostname, port = i_sess_identity("current")
    clusterAdminPassword = shell.prompt("Please provide the password for '" + clusterAdmin + "' : ", {'type':'password'})
    recovery_user = shell.prompt("Which user do you want to use for distributed recovery ? : ")
    recovery_password = shell.prompt("Please provide the password for '" + recovery_user + "' : ", {'type':'password'})

    donor_server = shell.prompt("\nWhich InnoDB Cluster's \033[94mserver name\033[0m do you want to use for cloning donor ? : ")
    donor_port = shell.prompt("\nWhich \033[94mport number\033[0m do you want to connect to the cloning donor ? : ")
    
    autoFlipProcess = True
    autoCloneProcess = True 

    x = shell.get_session()
    try:
        y = shell.open_session(clusterAdmin + "@" + donor_server + ":" + donor_port, clusterAdminPassword)
        shell.set_session(y)
    except:
        print("\n\033[1mERROR:\033[0m Unable to connect to donor database \n")
        shell.set_session(x)
        return
   
    print("\n\n\033[92m********** STEP 1 : Collect local Group Replication Nodes **********\033[0m\n")

    shell.set_session(x)
    node_list = []
    node_list.append(hostname + ":" + port)
    print(hostname + "@" + port + "\n")
    for node in i_get_other_node():
        try:
           if shell.parse_uri(node)["port"] > 10000:
               n = node[:-1]
           else:
               n = shell.parse_uri(node)["host"] + ":" + str(shell.parse_uri(node)["port"] - 10)
           z=shell.open_session(clusterAdmin + "@" + n, clusterAdminPassword)
           shell.set_session(z)
           node_list.append(n) 
           print(n + "\n")
        except:
           print("\033[1mINFO: \033[0m Unable to connect to '" + node + "', SKIPPED!")

    print("\n\n\033[92m********** STEP 2 : Dissolve local Group Replication **********\033[0m\n")
    shell.set_session(x)
    dissolve()

    try:
        result = i_run_sql("select channel_name from mysql_gr_replication_metadata.channel;","[']",False)
        if len(result) > 0:
            channel_name=result[0]
            stopMultiClusterChannel(channel_name)
        print("\n\033[1mINFO:\033[0m replication from InnoDB Cluster is INNACTIVE \n")
    except:
        print("\n\033[1mINFO:\033[0m replication from InnoDB Cluster is INNACTIVE \n")

    print("\n\033[92m********** STEP 3 : Clone instance from InnoDB Cluster **********\033[0m\n")
    shell.set_session(y)
    dba.get_cluster().add_instance(clusterAdmin + "@" + node_list[0], {'recoveryMethod':'clone'})
    time.sleep(10)
    dba.get_cluster().remove_instance(clusterAdmin + "@" + node_list[0])
    
    print("\n\033[92m********** STEP 4 : Create a Group Replication **********\033[0m\n")
    time.sleep(10)
    
    x = shell.open_session(clusterAdmin + "@" + hostname + ":" + port, clusterAdminPassword)
    shell.set_session(x)
    dissolve()
    
    
    create()
    
    print("\n\033[92m********** STEP 5 : Adding Instances back to Group Replication **********\033[0m\n")
    for i in range(len(node_list)):
        if i>0:
           addInstance(clusterAdmin + "@" + node_list[i])

    clusterAdminPassword = None
    recovery_user = None
    recovery_password = None
    autoFlipProcess = None
    autoCloneProcess = None
