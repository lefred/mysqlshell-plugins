def __getHeatWaveStatus(session=None):

    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    result = session.run_sql("""
        select 
            max(    case variable_name    when 'rapid_service_status' then variable_value    else ''    end ) as rapid_service_status, 
            max(    case variable_name    when 'rapid_cluster_status' then variable_value    else ''    end ) as rapid_cluster_status,
            max(    case variable_name    when 'rapid_plugin_bootstrapped' then variable_value    else ''    end ) as rapid_plugin_bootstrapped
        from performance_schema.global_status where variable_name like 'rapid%'
    """ )

    if (result.get_warnings_count() > 0):
        # Bail out and print the warnings
        print("Warnings occurred - bailing out:")
        print(result.get_warnings())
        return False


    rows = result.fetch_all()
    return rows

def __isHeatWaveOnline(session=None):

    rows = __getHeatWaveStatus(session)
    if len(rows) > 0 and rows[0][0] == "ONLINE":
        return True

    print("Heatwave is OFFLINE")
    return False

def __isHeatWavePlugin(session=None):

    rows = __getHeatWaveStatus(session)
    
    if len(rows) > 0 :
        if rows[0][2] == "NO":
            print("Heatwave Plugin not installed")
            return False
        else:
            return True

    return False

