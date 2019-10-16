# schema/default.py
# -----------------
# Definition of member functions for the schema extension object to display default values
#
# Usage example:
# --------------
#
#  MySQL  > 2019-06-26 18:07:50 > 
# JS> ext.schema.showDefaults('testing')
# No session specified. Either pass a session object to this function or connect the shell to a database
#  MySQL  > 2019-06-26 18:07:52 > 
# JS> \c root@localhost
# Creating a session to 'root@localhost'
# Fetching schema names for autocompletion... Press ^C to stop.
# Your MySQL connection id is 15 (X protocol)
# Server version: 8.0.16 MySQL Community Server - GPL
# No default schema selected; type \use <schema> to set one.
# MySQL 8.0.16 > > localhost:33060+ > > 2019-06-26 18:08:01 > 
# JS> ext.schema.showDefaults('testing')
# No schema specified. Either pass a schema name or use one
# MySQL 8.0.16 > > localhost:33060+ > > 2019-06-26 18:08:04 > 
# JS> ext.schema.showDefaults('testing', 'big')
# +--------------------------------+-----------------+----------------------------------------------------+---------------------------+
# | ColumnName                     | Type            | Default                                            | Example                   |
# +--------------------------------+-----------------+----------------------------------------------------+---------------------------+
# | id                             | int             | None                                               | NULL                      |
# | varchar_val                    | varchar         | None                                               | NULL                      |
# +--------------------------------+-----------------+----------------------------------------------------+---------------------------+
# Total: 2
# 
# MySQL 8.0.16 > > localhost:33060+ > > > test > 2019-06-26 18:11:01 > 
# JS> ext.schema.showDefaults('default_test')
# +--------------------------------+-----------------+----------------------------------------------------+---------------------------+
# | ColumnName                     | Type            | Default                                            | Example                   |
# +--------------------------------+-----------------+----------------------------------------------------+---------------------------+
# | bi_col_exp                     | bigint          | (8 * 8)                                            | 64                        |
# | d_col                          | date            | curdate()                                          | 2019-06-26                |
# | d_col_exp                      | date            | (curdate() + 8)                                    | 20190634                  |
# | dt_col                         | datetime        | CURRENT_TIMESTAMP                                  | 2019-06-26 18:11:16       |
# | vc_col_exp                     | varchar         | concat(_utf8mb4\'test\',_utf8mb4\'test\')          | testtest                  |
# +--------------------------------+-----------------+----------------------------------------------------+---------------------------+
# Total: 5
#

def __returnDefaults(session, schema, table):
    # Define the query to get the routines
    stmt = """SELECT COLUMN_NAME ColName, COLUMN_TYPE DataType, COLUMN_DEFAULT
              FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND
              TABLE_NAME = '%s'""" % (schema, table)

    # Execute the query and check for warnings
    result = session.run_sql(stmt)
    defaults = result.fetch_all()
    if (result.get_warnings_count() > 0):
        # Bail out and print the warnings
        print("Warnings occurred - bailing out:")
        print(result.get_warnings())
        return False

    return defaults


def show_defaults(table, schema=None, session=None):
    # Get hold of the global shell object
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return
    #if not session.uri.startswith('mysqlx'):
    #        print("The session object is not using X Protocol, please connect using mysqlx.")
    #        return
    if schema is None:
        if session.current_schema is None:
            print("No schema specified. Either pass a schema name or use one")
            return
        schema = session.current_schema.name

    defaults = __returnDefaults(session, schema, table)

    fmt = "| {0:30s} | {1:15s} | {2:50s} | {3:25s} |"
    header = fmt.format("ColumnName", "Type", "Default", "Example")
    bar = "+" + "-" * 32 + "+" + "-" * 17 + "+" + "-" * 52 + "+" + "-" * 27 + "+"
    print (bar)
    print (header)
    print (bar)
    for row in defaults:
        col_expr = row[2]
        if col_expr is None:
            col_expr = 'NULL'
        else:
            col_expr = col_expr.replace('\\', '')
        query = session.run_sql("select {0}".format(col_expr))
        if col_expr == 'NULL':
            ex_str = col_expr
        else:
            try:
                example = query.execute()
                for col in example.fetch_all():
                    ex_str = str(col[0])
            except:
                ex_str = row[2]
        print (fmt.format(row[0], row[1], col_expr, ex_str))
    print (bar)

    return "Total: %d" % len(defaults)
