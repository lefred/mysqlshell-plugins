from mysqlsh_plugins_common import run_and_show
from mysqlsh.plugin_manager import plugin, plugin_function

@plugin_function("check.getAmountDDL")
def get_amount_ddl(session=None):
    """
    Prints a summary of the amount of DDL statements performed since server start.

    This function list all the amount of DDL statements performed on the MySQL
    server since its start.

    Args:
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    """

    stmt = """SELECT event_name, count_star, sum_errors 
              FROM performance_schema.events_statements_summary_global_by_event_name 
              WHERE event_name  REGEXP '.*sql/(create|drop|alter).*' 
                AND event_name NOT REGEXP '.*user'
                AND count_star > 0;"""

    run_and_show(stmt, "table", session)
