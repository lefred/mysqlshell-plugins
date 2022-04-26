import support.collections.common as common
common.collectList.append("replication.collect")

def collect(session, header, minute_cpt):
    stmt = """SELECT
  conn_status.channel_name as channel_name,
  conn_status.service_state as IO_thread,
  applier_status.service_state as SQL_thread,
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
      timediff(APPLYING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP,now()))))) `lag_in_sec`
FROM
  performance_schema.replication_connection_status AS conn_status
JOIN performance_schema.replication_applier_status_by_worker AS applier_status
  ON applier_status.channel_name = conn_status.channel_name"""
    common._run_me(session, stmt, header, "replication.txt")

    return