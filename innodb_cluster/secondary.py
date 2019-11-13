from mysqlsh import mysql
import time

def _get_secondary(session):
       stmt = """select concat(member_host,':',member_port) `member` 
                 from performance_schema.replication_group_members 
                 where member_role='SECONDARY'"""
       result = session.run_sql(stmt)
       secondaries = []
       rows = result.fetch_all()
       for row in rows:
         secondaries.append(row[0])
       secondaries.sort() 
       return secondaries

def _get_members(session):
       stmt = """select concat(member_host,':',member_port) `member` 
                 from performance_schema.replication_group_members"""
       result = session.run_sql(stmt)
       members = []
       rows = result.fetch_all()
       for row in rows:
         members.append(row[0])
       members.sort() 
       return members

def _check_session_cred(shell, uri):
    tofind = uri.split('/')[2]
    print(uri)
    if tofind not in shell.list_credentials():
        print("%s is not stored in your Shell Credentials")
        answer = shell.prompt('Do you want to connect to it now ? (Y/n) ', {'defaultValue':'y'})
        if answer.lower() == 'y':
            session_sec = shell.connect(uri)
            session_sec.close()
    return

def _connect_to_secondary(shell, session, secondary):
    uri_json = shell.parse_uri(session.get_uri())
    uri_json['host'] = secondary.split(':')[0]
    uri_json['port'] = secondary.split(':')[1] 
    uri_json['scheme'] = "mysql"
    uri = shell.unparse_uri(uri_json)
    
    #print("Using %s to connect to secondary..." % uri)
    #_check_session_cred(shell, uri)
    password = shell.prompt('Please enter password for %s: ' % uri, {'type': 'password'})
    session2 = mysql.get_session(uri, password)
    return session2

def _get_cluster_mode(session, dba):
   return dba.get_cluster().status()['defaultReplicaSet']['topologyMode']

def show_speed(limit=10,session=None):

    import mysqlsh
    shell = mysqlsh.globals.shell
    secondary_sessions = {}

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return
    cluster_mode = _get_cluster_mode(session, mysqlsh.globals.dba)
   

    if cluster_mode == 'Single-Primary':
       secondaries = _get_secondary(session)
    else: 
       secondaries = _get_members(session)

    if len(secondaries) == 0:
        print("No Secondary Members discovered !")
        return
    out = ""
    head0 = ""
    head1 = ""
    head2 = ""
    for secondary in secondaries:
        secondary_sessions[secondary] = _connect_to_secondary(shell, session, secondary)
    head0 = "		SeqNo				SeqNo							"
    head1 = "		Last				Last		transport	time to				"
    head2 = "Host		ApplyTx		repl delay	QueueTx		time		RelayLog	apply time	"
    print("=" * 106)
    print("MySQL InnoDB Cluster : Group Replication Information")
    print("=" * 106)
    print(head0)
    print(head1)
    print(head2)
    print("-" * 106)
    
    for i in range(limit):
            out = ""
            for secondary in secondaries:
               stmt = """SELECT if(LAST_APPLIED_TRANSACTION, LAST_APPLIED_TRANSACTION, "0:none") LAST_APPLIED_TRANSACTION, 
                         LAST_QUEUED_TRANSACTION, LAST_APPLIED_TRANSACTION_END_APPLY_TIMESTAMP - 
	                    LAST_APPLIED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP 'rep delay (sec)', 
                         LAST_QUEUED_TRANSACTION_START_QUEUE_TIMESTAMP - 
                            LAST_QUEUED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP 'transport time', 
                         LAST_QUEUED_TRANSACTION_END_QUEUE_TIMESTAMP - 
                            LAST_QUEUED_TRANSACTION_START_QUEUE_TIMESTAMP 'time RL',
                         LAST_APPLIED_TRANSACTION_END_APPLY_TIMESTAMP - 
                            LAST_APPLIED_TRANSACTION_START_APPLY_TIMESTAMP 'apply time'
                         FROM performance_schema.replication_applier_status_by_worker t1 
                         JOIN performance_schema.replication_connection_status t2 
                           ON t2.channel_name=t1.channel_name  
                        WHERE t1.channel_name='group_replication_applier'"""
               result = secondary_sessions[secondary].run_sql(stmt)
               row = result.fetch_one()
               if int(row[2].split('.')[0]) > 2019111308000:
                    delay_time="n/a	"
               else:
                    delay_time=row[2]
               if int(row[3].split('.')[0]) > 2019111308000:
                    trans_time="n/a	"
               else:
                    trans_time=row[3]
               out = "%s	%s		%s	%s		%s	%s	%s	" % (secondary, row[0].split(':')[1], delay_time, row[1].split(':')[1], trans_time, row[4], row[5]) 
               print(out) 
            print(" ")
            time.sleep(1)
