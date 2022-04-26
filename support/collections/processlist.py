import support.collections.common as common
common.collectList.append("processlist.collect")

def collect(session, header, minute_cpt):
    stmt = """select unix_timestamp() as `timestamp`, t1.* from sys.processlist t1 
              where pid is not NULL"""
    common._run_me(session, stmt, header, "processlist.txt")

    return
