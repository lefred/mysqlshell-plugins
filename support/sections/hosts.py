from support.sections import util

def _get_blocked_hosts(session, advices, details):
    title = "Blocked Hosts"
    stmt = """select host, ip, COUNT_HOST_BLOCKED_ERRORS, @@max_connect_errors 
    from performance_schema.host_cache where COUNT_HOST_BLOCKED_ERRORS>0;
    """
    result = session.run_sql(stmt)
    if result.has_data():
        rows = result.fetch_all()
        if len(rows) > 0:
            output = util.output(title, "")
            for row in rows:
                output += util.output("{} ({})".format(row[0], row[1]), row[2], 1)
            if advices:
                output += util.print_red("You have blocked host(s), please use mysqladmin flush-hosti and maybe increase max_connect_errors ({}).".format(row[3]))
            if details:
                stmt = """select HOST, IP, COUNT_HOST_BLOCKED_ERRORS, FIRST_SEEN, LAST_ERROR_SEEN 
                      from performance_schema.host_cache where COUNT_HOST_BLOCKED_ERRORS>0
                    """
                stmt = """SELECT HOST, IP, COUNT_HOST_BLOCKED_ERRORS, concat("'", FIRST_SEEN, "'") FIRST_SEEN,
                          concat("'", LAST_SEEN, "'") LAST_SEEN
                          FROM performance_schema.host_cache where COUNT_HOST_BLOCKED_ERRORS>0"""
                output2, nbrows = util.run_and_print("Blocked Hosts", stmt, session)
                output += output2
        else:
            output = util.output(title, "none")

    return output


def get_host_info(session, advices, details, branch):
    output =_get_blocked_hosts(session, advices, details)
    output += "\n"
    return output
