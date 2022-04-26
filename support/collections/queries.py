import support.collections.common as common
common.collectList.append("queries.collect")

def collect(session, header, minute_cpt):
    if (minute_cpt == 1) or (minute_cpt % 60 == 0):
        stmt = """SELECT unix_timestamp() as `timestamp`, schema_name, format_pico_time(total_latency) tot_lat,   
       exec_count, format_pico_time(total_latency/exec_count) latency_per_call, 
       query_sample_text 
  FROM sys.x$statements_with_runtimes_in_95th_percentile AS t1
  JOIN performance_schema.events_statements_summary_by_digest AS t2 
    ON t2.digest=t1.digest 
 WHERE schema_name NOT in ('performance_schema', 'sys') 
ORDER BY (total_latency/exec_count) desc LIMIT 10"""
        common._run_me(session, stmt, header, "queries.txt")
    return
