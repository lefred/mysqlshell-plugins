from ext.mysqlsh_plugins_common import run_and_show

def get_amount_ddl(session=None):

    stmt = """SELECT event_name, count_star, sum_errors 
              FROM performance_schema.events_statements_summary_global_by_event_name 
              WHERE event_name  REGEXP '.*sql/(create|drop|alter).*' 
                AND event_name NOT REGEXP '.*user'
                AND count_star > 0;"""

    run_and_show(stmt, session)
