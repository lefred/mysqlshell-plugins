import support.collections.common as common
common.collectGlobalList.append("info.collect")


def collect(session, header):
    stmt = "select version()"
    common._run_me(session, stmt, header, "global_info.txt")