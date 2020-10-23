from mysqlsh.plugin_manager import plugin, plugin_function
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
    #print(uri)
    if tofind not in shell.list_credentials():
        print("%s is not stored in your Shell Credentials")
        answer = shell.prompt(
            'Do you want to connect to it now ? (Y/n) ', {'defaultValue': 'y'})
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
    #password = shell.prompt('Please enter password for %s: ' % uri, {'type': 'password'})
    #session2 = mysql.get_session(uri, password)
    session2 = shell.open_session(uri)
    return session2


def _get_cluster_mode(session, dba):
    return dba.get_cluster().status()['defaultReplicaSet']['topologyMode']


@plugin_function("innodb_cluster.showGroupReplicationSpeed")
def show_speed(limit=10, session=None):
    """
    Prints replication information speed information.

    This function will connect to all members of the InnoDB Cluster and display the speed
    of Group Replication.

    Args:
        limit (integer): For how many seconds the output show be displayed, refreshed every second (default: 10).
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    """
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
        secondary_sessions[secondary] = _connect_to_secondary(
            shell, session, secondary)
    head0 = "		SeqNo     SeqNo							"
    head1 = "		Last      Last		        transport       time to		                Lag"
    head2 = "Host		QueueTx   ApplyTx  repl delay	time            RelayLog	apply time	in sec"
    print("=" * 102)
    print("MySQL InnoDB Cluster : Group Replication Information")
    print("=" * 102)
    print(head0)
    print(head1)
    print(head2)
    print("-" * 102)

    for i in range(limit):
        out = ""
        for secondary in secondaries:
            stmt = """SELECT
                        conn_status.LAST_QUEUED_TRANSACTION as last_queued_transaction,
                        applier_status.LAST_APPLIED_TRANSACTION as last_applied_transaction,
                        if(LAST_APPLIED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP = 0, 0,
                           LAST_APPLIED_TRANSACTION_END_APPLY_TIMESTAMP -
                            LAST_APPLIED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP) 'rep delay (sec)',
                        LAST_QUEUED_TRANSACTION_START_QUEUE_TIMESTAMP -
                           LAST_QUEUED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP 'transport time',
                           LAST_QUEUED_TRANSACTION_END_QUEUE_TIMESTAMP -
                           LAST_QUEUED_TRANSACTION_START_QUEUE_TIMESTAMP 'time RL',
                        LAST_APPLIED_TRANSACTION_END_APPLY_TIMESTAMP -
                           LAST_APPLIED_TRANSACTION_START_APPLY_TIMESTAMP 'apply time',
                        if(GTID_SUBTRACT(LAST_QUEUED_TRANSACTION, LAST_APPLIED_TRANSACTION) = "","0" ,
                        abs(time_to_sec(if(time_to_sec(APPLYING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP)=0,0,
                           timediff(APPLYING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP,now())))))`lag`
                        FROM
                           performance_schema.replication_connection_status AS conn_status
                           JOIN performance_schema.replication_applier_status_by_worker AS applier_status
                           ON applier_status.channel_name = conn_status.channel_name
                           WHERE conn_status.channel_name='group_replication_applier'"""
            result = secondary_sessions[secondary].run_sql(stmt)
            row = result.fetch_one()
            out = "%s	%s       %s       %s	%s	%s	%s	%s" % (secondary, row[0].split(
                ':')[1], row[1].split(':')[1], row[2], row[3], row[4], row[5], row[6])
            print(out)
        print(" ")
        time.sleep(1)
