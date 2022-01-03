from mysqlsh.plugin_manager import plugin, plugin_function
from heatwave_utils.comm import __isHeatWaveOnline, __isHeatWavePlugin
from heatwave_utils import comm

def __returnQueryStats(session):
   
    # Define the query to get the routines
    stmt = """SELECT query_id, left(query_text,40) AS `query`,
       JSON_EXTRACT(JSON_UNQUOTE(qkrn_text->'$**.sessionId'),'$[0]') AS session_id,
       JSON_EXTRACT(JSON_UNQUOTE(qkrn_text->'$**.totalBaseDataScanned'), '$[0]') AS data_scanned, 
       JSON_EXTRACT(JSON_UNQUOTE(qexec_text->'$**.error'),'$[0]') AS error_message 
       FROM performance_schema.rpd_query_stats
    """


    stmt = stmt + ";"
    # Execute the query and check for warnings
    result = session.run_sql(stmt)
    return result;

def __returnTraceInfo(session):
   
    # Define the query to get the routines
    stmt = """SELECT QUERY `Query`, TRACE->'$**.Rapid_Offload_Fails' 'Offload Failed',
                     if(TRACE->'$**.secondary_engine_not_used' is NULL,
                               if(TRACE->'$**.secondary_engine_cost' is NULL,
                                  'True', JSON_PRETTY(TRACE->>'$**.secondary_engine_not_used')),
                        JSON_PRETTY(TRACE->>'$**.secondary_engine_not_used')) 'HeatWave Not Used'
               FROM INFORMATION_SCHEMA.OPTIMIZER_TRACE
           """

    # Execute the query and check for warnings
    result = session.run_sql(stmt)
    return result;


@plugin_function("heatwave_utils.report_query_stats")
def report_query_stats(session=None):
    """
    Wizard to report query stats

    Args:
        session (object): The optional session object

    """
    # Get hold of the global shell object
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    if __isHeatWaveOnline(session)  :
        result= __returnQueryStats(session )
        shell.dump_rows(result) 

    return ;

@plugin_function("heatwave_utils.report_trace_info")
def report_trace_info(session=None):
    """
    Wizard to report trace info

    Args:
        session (object): The optional session object

    import mysqlsh
    shell = mysqlsh.globals.shell
    """
    # Get hold of the global shell object
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return
    if comm.mytrace is False :
        print("Trace is not ON, please run heatwave_utils.set_trace_on()")
        return

#    if __isHeatWaveOnline(session)  :
    if comm.mytrace :
        result= __returnTraceInfo(session)
        shell.dump_rows(result, "vertical")

    return ;
