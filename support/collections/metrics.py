import support.collections.common as common
common.collectList.append("metrics.collect")

def collect(session, header, minute_cpt):
    stmt = "select unix_timestamp() as `timestamp`, t1.* from sys.metrics as t1"
    common._run_me(session, stmt, header, "metrics.txt")
    return
