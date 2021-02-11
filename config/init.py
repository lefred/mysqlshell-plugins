from mysqlsh.plugin_manager import plugin, plugin_function
from mysqlsh_plugins_common import run_and_show

@plugin
class config:
    """
    MySQL configuration utility.

    A collection of tools to get information on your
    MySQL Database Server's configuration variables.
    """

@plugin_function("config.getGlobalNonDefault")
def get_global_non_default(format="table",session=None):
    """
    Prints all global variables being configured or changed.

    Args:
        format (string): One of table, tabbed, vertical, json, ndjson, json/raw,
              json/array, json/pretty. Default is table.
              In table format, the value column is truncated to 50 characters.
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    """
    if format == "table":
        stmt = """SELECT t1.VARIABLE_NAME "Variable Name", concat(set_user, "@", set_host) `Changed by`,
                  set_time `Time`, substring(VARIABLE_VALUE,1, 50) `Value`, VARIABLE_SOURCE `Source`
                  FROM performance_schema.variables_info t1
                  JOIN performance_schema.global_variables t2
                    ON t2.VARIABLE_NAME=t1.VARIABLE_NAME
                 WHERE t1.VARIABLE_SOURCE != 'COMPILED';"""
    else:
        stmt = """SELECT t1.VARIABLE_NAME "Variable Name", concat(set_user, "@", set_host) `Changed by`,
                  set_time `Time`, VARIABLE_VALUE `Value`, VARIABLE_SOURCE `Source`
                  FROM performance_schema.variables_info t1
                  JOIN performance_schema.global_variables t2
                    ON t2.VARIABLE_NAME=t1.VARIABLE_NAME
                 WHERE t1.VARIABLE_SOURCE != 'COMPILED';"""

    run_and_show(stmt, format, session)


@plugin_function("config.getSessionNonDefault")
def get_session_non_default(format="table",session=None):
    """
    Prints all session variables being changed or configured.

    Args:
        format (string): One of table, tabbed, vertical, json, ndjson, json/raw,
              json/array, json/pretty. Default is table.
              In table format, the value column is truncated to 50 characters.
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    """
    if format == "table":
        stmt = """SELECT t1.VARIABLE_NAME "Variable Name", concat(set_user, "@", set_host) `Changed by`,
                  set_time `Time`, substring(VARIABLE_VALUE,1, 50) `Value`, VARIABLE_SOURCE `Source`
                  FROM performance_schema.variables_info t1
                  JOIN performance_schema.session_variables t2
                    ON t2.VARIABLE_NAME=t1.VARIABLE_NAME
                 WHERE t1.VARIABLE_SOURCE = 'DYNAMIC';"""
    else:
        stmt = """SELECT t1.VARIABLE_NAME "Variable Name", concat(set_user, "@", set_host) `Changed by`,
                  set_time `Time`, VARIABLE_VALUE `Value`, VARIABLE_SOURCE `Source`
                  FROM performance_schema.variables_info t1
                  JOIN performance_schema.session_variables t2
                    ON t2.VARIABLE_NAME=t1.VARIABLE_NAME
                 WHERE t1.VARIABLE_SOURCE = 'DYNAMIC';"""

    run_and_show(stmt, format, session)


@plugin_function("config.getPersistedVariables")
def get_persisted_variables(format="table",session=None):
    """
    Prints all variables that have been persisted.

    Args:
        format (string): One of table, tabbed, vertical, json, ndjson, json/raw,
              json/array, json/pretty. Default is table.
              In table format, the value column is truncated to 50 characters.
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    """
    if format == "table":
        stmt = """SELECT t1.VARIABLE_NAME "Variable Name", concat(set_user, "@", set_host) `Changed by`,
                  set_time `Time`, substring(VARIABLE_VALUE,1, 50) `Value`, VARIABLE_SOURCE `Source`
                  FROM performance_schema.variables_info t1
                  JOIN performance_schema.global_variables t2
                    ON t2.VARIABLE_NAME=t1.VARIABLE_NAME
                 WHERE t1.VARIABLE_SOURCE = 'PERSISTED';"""
    else:
        stmt = """SELECT t1.VARIABLE_NAME "Variable Name", concat(set_user, "@", set_host) `Changed by`,
                  set_time `Time`, VARIABLE_VALUE `Value`, VARIABLE_SOURCE `Source`
                  FROM performance_schema.variables_info t1
                  JOIN performance_schema.global_variables t2
                    ON t2.VARIABLE_NAME=t1.VARIABLE_NAME
                 WHERE t1.VARIABLE_SOURCE = 'PERSISTED';"""

    run_and_show(stmt, format, session)

@plugin_function("config.getVariableInfo")
def get_vatiable_info(variable_name, format="table", session=None):
    """
    Prints all variables that have been persisted.

    Args:
        variable_name (string): The variable which you want to display the info.
        format (string): One of table, tabbed, vertical, json, ndjson, json/raw,
              json/array, json/pretty. Default is table.
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    """
    stmt = """SELECT t1.*, VARIABLE_VALUE
       FROM performance_schema.variables_info t1
       JOIN performance_schema.global_variables t2
         ON t2.VARIABLE_NAME=t1.VARIABLE_NAME
      WHERE t1.VARIABLE_NAME LIKE '{}'""".format(variable_name)

    run_and_show(stmt, format, session)