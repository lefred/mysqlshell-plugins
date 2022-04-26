import support.collections.common as common
common.collectList.append("innodb_status.collect")

def collect(session, header, minute_cpt):
    if (minute_cpt == 1) or (minute_cpt % 60 == 0):
        stmt = "show engine innodb status"
        common._run_me(session, stmt, header, "innodb_status.txt")
    return
