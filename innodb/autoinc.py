from mysqlsh.plugin_manager import plugin, plugin_function
from mysqlsh_plugins_common import run_and_show


@plugin_function("innodb.getAutoincFill")
def get_autoinc_fill(percentage=50, schema=None, session=None):
    """
    Prints information about auto_increment fill up.

    Args:
        percentage (integer): Only shows the tables where auto increments
                              values are filled to at least % (default: 50).
        schema (string): The name of the schema to use. This is optional.
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    """
    where_filter = ""
    having_filter = ""

    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return
    stmt = "select @@information_schema_stats_expiry;"
    result = session.run_sql(stmt)
    stats = result.fetch_all()
    if len(stats) > 0:
        for stat in stats:
            if int(stat[0]) > 0:
                print(
                    "Warning: information_schema_stats_expiry is set to {0}.".format(*stat))
                if shell.options.interactive:
                    answer = shell.prompt(
                        """Do you want to change it ? (y/N) """, {'defaultValue': 'n'})
                    if answer.lower() == 'y':
                        stmt = """SET information_schema_stats_expiry=0"""
                        result = session.run_sql(stmt)
                else:
                    print(
                        "Changing information_schema_stats_expiry to 0 for this session only")
                    stmt = """SET information_schema_stats_expiry=0"""
                    result = session.run_sql(stmt)

    if percentage > 0:
        having_filter = "HAVING CAST(AUTO_INCREMENT_RATIO AS SIGNED INTEGER) >= {}".format(
            percentage)
    if schema:
        where_filter = "{} AND TABLE_SCHEMA='{}'".format(where_filter, schema)

    stmt = """SELECT
        TABLE_SCHEMA,
        TABLE_NAME,
        COLUMN_NAME,
        DATA_TYPE,
        COLUMN_TYPE,
        IF(
        LOCATE('unsigned', COLUMN_TYPE) > 0,
        1,
        0
        ) AS IS_UNSIGNED,
        (
        CASE DATA_TYPE
        WHEN 'tinyint' THEN 255
        WHEN 'smallint' THEN 65535
        WHEN 'mediumint' THEN 16777215
        WHEN 'int' THEN 4294967295
        WHEN 'bigint' THEN 18446744073709551615
        END >> IF(LOCATE('unsigned', COLUMN_TYPE) > 0, 0, 1)
        ) AS MAX_VALUE,
        AUTO_INCREMENT, CONCAT(ROUND(
        AUTO_INCREMENT / (
        CASE DATA_TYPE
        WHEN 'tinyint' THEN 255
        WHEN 'smallint' THEN 65535
        WHEN 'mediumint' THEN 16777215
        WHEN 'int' THEN 4294967295
        WHEN 'bigint' THEN 18446744073709551615
        END >> IF(LOCATE('unsigned', COLUMN_TYPE) > 0, 0, 1)
        )*100), '%') AS AUTO_INCREMENT_RATIO
        FROM
        INFORMATION_SCHEMA.COLUMNS
        INNER JOIN INFORMATION_SCHEMA.TABLES USING (TABLE_SCHEMA, TABLE_NAME)
        WHERE
        TABLE_SCHEMA NOT IN ('mysql', 'INFORMATION_SCHEMA', 'performance_schema')
        AND EXTRA='auto_increment' {}
        {}
        ORDER BY CAST(AUTO_INCREMENT_RATIO AS SIGNED INTEGER)
        """.format(where_filter, having_filter)

    run_and_show(stmt)

    return
