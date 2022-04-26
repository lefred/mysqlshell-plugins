import support.collections.common as common
common.collectList.append("host_summary.collect")

def collect(session, header, minute_cpt):
    if (minute_cpt == 1) or (minute_cpt % 60 == 0):
      stmt = "select unix_timestamp() as `timestamp`, t1.* from sys.host_summary as t1"
      common._run_me(session, stmt, header, "host_summary.txt")
    return
