import support.collections.common as common
common.collectList.append("status.collect")

def collect(session, header, minute_cpt):
    stmt = "select unix_timestamp() as `timestamp`, t1.* from performance_schema.status_by_user as t1"
    common._run_me(session, stmt, header, "status_by_user.txt")
    stmt = "select unix_timestamp() as `timestamp`, t1.* from performance_schema.status_by_host as t1"
    common._run_me(session, stmt, header, "status_by_host.txt")
    stmt = "select unix_timestamp() as `timestamp`, t1.* from performance_schema.status_by_thread as t1"
    common._run_me(session, stmt, header, "status_by_thread.txt")
    stmt = "select unix_timestamp() as `timestamp`, t1.* from performance_schema.status_by_account as t1"
    common._run_me(session, stmt, header, "status_by_account.txt")
    stmt = "show global status"
    common._run_me(session, stmt, header, "global_status.txt")
    return
