from mysqlsh.plugin_manager import plugin, plugin_function

@plugin_function("innodb.getCheckpointAge")
def get_checkpoint_age(session=None):
    """
    Returns the InnoDB Checkpoint Age

    Args:
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

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

    stmt="""SELECT CONCAT(
                (SELECT FORMAT_BYTES(STORAGE_ENGINES->>'$."InnoDB"."LSN"' - STORAGE_ENGINES->>'$."InnoDB"."LSN_checkpoint"')
                        FROM performance_schema.log_status)
                , " / ",
                format_bytes(
                    (SELECT VARIABLE_VALUE
                            FROM performance_schema.global_variables WHERE VARIABLE_NAME = 'innodb_log_file_size')
                    *
                    (SELECT VARIABLE_VALUE
                            FROM performance_schema.global_variables WHERE VARIABLE_NAME = 'innodb_log_files_in_group'))
                ),
                (SELECT ROUND(((
                    SELECT STORAGE_ENGINES->>'$."InnoDB"."LSN"' - STORAGE_ENGINES->>'$."InnoDB"."LSN_checkpoint"'
                    FROM performance_schema.log_status) / ((
                    SELECT VARIABLE_VALUE
                    FROM performance_schema.global_variables
                    WHERE VARIABLE_NAME = 'innodb_log_file_size')
                    *
                    (SELECT VARIABLE_VALUE
                     FROM performance_schema.global_variables
                     WHERE VARIABLE_NAME = 'innodb_log_files_in_group')) * 100),2))
                `Checkpoint Age`,
                format_bytes(
                    (SELECT VARIABLE_VALUE
                     FROM performance_schema.global_variables
                     WHERE variable_name = 'innodb_log_file_size')) `InnoDB Log File Size`,
                    (SELECT VARIABLE_VALUE
                     FROM performance_schema.global_variables
                     WHERE variable_name = 'innodb_log_files_in_group') `Nb of files`,
                (SELECT VARIABLE_VALUE FROM performance_schema.global_status
                  WHERE VARIABLE_NAME = 'Innodb_redo_log_enabled') `Redo`
         """
    (chkpoint_age, chkpoint_age_pct, log_file_size, nb_files, redo) = session.run_sql(stmt).fetch_one()
    if redo == 'OFF':
        print("\033[31mInnoDB Redo Logs are disabled !!\033[0m")
    if int(float(chkpoint_age_pct)) >= 80:
        chkpoint_age_pct = "\033[31m{}%\033[0m".format(chkpoint_age_pct)
    elif int(float(chkpoint_age_pct)) >= 75:
        chkpoint_age_pct = "\033[33m{}%\033[0m".format(chkpoint_age_pct)
    else:
        chkpoint_age_pct = "{}%".format(chkpoint_age_pct)
    print("InnoDB is using {} files of {}".format(nb_files, log_file_size))
    print("InnoDB Checkpoint Age: {} ({})".format(chkpoint_age, chkpoint_age_pct))

    return
