import re
from support.sections import util
from user.mds import mds_allowed_privileges
from user.mds import mds_allowed_auth_plugins

def version_info(session):
    supported = False
    stmt = "select @@version_comment, @@version, @@version_compile_machine"
    result = session.run_sql(stmt)
    row = result.fetch_one()
    type = _get_type_by_pid(session)
    output = util.output("MySQL Version", "{} ({}) - {} {}".format(row[0], row[1], row[2], type))
    if int(row[1][0]) >= 8 and row[0].startswith('MySQL'):
        supported = True
    version = row[1]
    stmt = "show variables like 'aurora_version'"
    result = session.run_sql(stmt)
    row = result.fetch_one()
    if row:
        output += util.output("Aurora", "{}".format(row[1]))
    return supported, output, version

def get_dataset(session, branch):
    stmt = """SELECT format_bytes(SUM(data_length)) Data,
                     format_bytes(SUM(index_length)) Indexes,
                     format_bytes(SUM(data_length)+sum(index_length)) 'Total Size'
              FROM information_schema.TABLES GROUP BY NULL"""
    if branch == "56":
        stmt = """SELECT CONCAT(ROUND(SUM(data_length) / ( 1024 * 1024 * 1024 ), 2), 'G') Data,
                     CONCAT(ROUND(SUM(index_length) / ( 1024 * 1024 * 1024 ), 2), 'G') Indexes,
                     CONCAT(ROUND(SUM(data_length+index_length)/( 1024 * 1024 * 1024 ), 2), 'G') 'Total Size'
              FROM information_schema.TABLES GROUP BY NULL"""
    else:
        if branch == "57":
            stmt = """SELECT sys.format_bytes(SUM(data_length)) Data,
                     sys.format_bytes(SUM(index_length)) Indexes,
                     sys.format_bytes(SUM(data_length)+sum(index_length)) 'Total Size'
              FROM information_schema.TABLES GROUP BY NULL"""

    result = session.run_sql(stmt)
    output = util.output("Dataset", "")
    while result.has_data():
        object_res = result.fetch_one_object()
        if object_res:
            for key in object_res:
                output += util.output(key, object_res[key], 1)
            result.next_result()
        else:
            break

    return output



def get_largest_innodb_tables(session, branch, details=False, limit=10):

    if not details:
        return ""

    title = "Top {} largest InnoDB Tables".format(str(limit))
    stmt = """SELECT NAME as `Table Name`, TABLE_ROWS as `Rows`, format_bytes(data_length) `Data Size`,
                     format_bytes(PAGE_SIZE) `Page Size`, SPACE_TYPE `Space Type`,
                     format_bytes(index_length) `Index Size`,
                     format_bytes(data_length+index_length) `Total Size`,
                     format_bytes(data_free) `Data Free`,
                     format_bytes(FILE_SIZE) `File Size`,
                     format_bytes((FILE_SIZE/10 - (data_length/10 +
                           index_length/10))*10) `Wasted Size`
                FROM information_schema.TABLES as t
                JOIN information_schema.INNODB_TABLESPACES as it
                ON it.name = concat(table_schema,"/",table_name)
                ORDER BY (data_length + index_length) desc limit %s""" % str(limit)
    if branch == "56":
        stmt = """SELECT CONCAT(table_schema, '.', table_name)  as `Table Name`, CONCAT(ROUND(table_rows / 1000000, 2), 'M') as `Rows`, 
                     CONCAT(ROUND(data_length / ( 1024 * 1024 * 1024 ), 2), 'G') `Data Size`,
                     CONCAT(ROUND(index_length / ( 1024 * 1024 * 1024 ), 2), 'G') `Index Size`,
                     CONCAT(ROUND(( data_length + index_length ) / ( 1024 * 1024 * 1024 ), 2), 'G') `Total Size`
                FROM information_schema.TABLES as t
                ORDER BY (data_length + index_length) desc limit %s""" % str(limit)
    else:
        if branch == "57":
            stmt = """SELECT TABLE_NAME as `Table Name`, TABLE_ROWS as `Rows`, sys.format_bytes(data_length) `Data Size`,
                     sys.format_bytes(index_length) `Index Size`,
                     sys.format_bytes(data_length+index_length) `Total Size`,
                     sys.format_bytes(data_free) `Data Free`
                FROM information_schema.TABLES as t
                ORDER BY (data_length + index_length) desc limit %s""" % str(limit)
                
    output, nbrows = util.run_and_print(title, stmt, session)
    return output

def get_tables_without_pk(session, advices=False, details=False):
    tbl_no_pk = 0
    title = "Tables without PK"
    stmt = """SELECT concat(tables.table_schema, '/' , tables.table_name) as `Table Name`, tables.engine as `Engine`,
       tables.table_rows as `Rows` FROM information_schema.tables  LEFT JOIN (
          SELECT table_schema , table_name
          FROM information_schema.statistics
          GROUP BY table_schema, table_name, index_name HAVING
           SUM( case when non_unique = 0 and nullable != 'YES' then 1 else 0 end ) = count(*) ) puks
           ON tables.table_schema = puks.table_schema and tables.table_name = puks.table_name
           WHERE puks.table_name is null
            AND tables.table_type = 'BASE TABLE' AND Engine='InnoDB'"""
    result = session.run_sql(stmt)
    output = util.output(title, "")
    for row in result.fetch_all():
        tbl_no_pk += 1

    output = util.output(title, tbl_no_pk)

    if advices:
        if tbl_no_pk > 0:
             output += util.print_red("It's not recommended to have tables without Primary Key")

    if details and tbl_no_pk > 0:
        output2, nbrows = util.run_and_print("Details", stmt, session)
        output += output2
         
    return output

def get_engines(session, advices=False, details=False):
    got_inno = False
    other = False
    title = "Engines Used"
    stmt = """select count(*) as '# TABLES', CONCAT(ROUND(sum(data_length) / ( 1024 * 1024 * 1024 ), 2), 'G') DATA, 
                     CONCAT(ROUND(sum(index_length) / ( 1024 * 1024 * 1024 ), 2), 'G') INDEXES,
                     CONCAT(sum(ROUND(( data_length + index_length ) / ( 1024 * 1024 * 1024 ), 2)), 'G') 'TOTAL SIZE', 
                     engine as ENGINE from information_schema.TABLES  
              where TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys') 
                and data_length group by engine;"""
    result = session.run_sql(stmt)
    output = util.output(title, "")
    for row in result.fetch_all():
        output += util.output(row[4], "{} table(s) ({})".format(row[0], row[3]), 1)
        if row[4] == "InnoDB":
            got_inno = True
        else:
            other = True

    if advices:
        if not got_inno and other: 
             output += util.print_red("It's recommended to only use InnoDB")

    if details:
        output2, nbrows = util.run_and_print("Details".format(title), stmt, session)
        output += output2

    return output

def get_configured_variables(session, details=False):
    title = "Global Variables Configured"
    stmt = """SELECT count(*), VARIABLE_SOURCE
               FROM performance_schema.variables_info t1
               JOIN performance_schema.global_variables t2
               ON t2.VARIABLE_NAME=t1.VARIABLE_NAME
               WHERE t1.VARIABLE_SOURCE != 'COMPILED' GROUP BY VARIABLE_SOURCE"""
    result = session.run_sql(stmt)
    output = util.output(title, "")
    for row in result.fetch_all():
        output += util.output(row[1], row[0], 1)


    if details:
        stmt = """SELECT t1.VARIABLE_NAME, VARIABLE_VALUE, VARIABLE_SOURCE 
               FROM performance_schema.variables_info t1 
               JOIN performance_schema.global_variables t2 
               ON t2.VARIABLE_NAME=t1.VARIABLE_NAME 
               WHERE t1.VARIABLE_SOURCE != 'COMPILED'"""
        output2, nbrows = util.run_and_print("Details".format(title), stmt, session)
        output += output2
    return output

def get_flush_commands(session, advices):
    title = "Flush Commands"
    stmt = "show global status like 'Com_flush'"
    result = session.run_sql(stmt)
    row = result.fetch_one()
    output = util.output(title, "{}".format(row[1]))
    if advices:
        if int(row[1]) > 0:
            output += util.print_orange("Pay attention that FLUSH commands like FLUSH PRIVILEGES will break MDS Inbound Replication")
            output += util.print_orange("Consider using FLUSH LOCAL instead")
    return output

def get_users_auth_plugins(session, advices=False):
    title = "Authentication Plugins"
    stmt = "select plugin, count(*) 'nb of users' from mysql.user group by plugin"
    result = session.run_sql(stmt)
    output = util.output("Authentication Plugins", "")
    for row in result.fetch_all():
        output += util.output(row[0], "{} user(s)".format(row[1]), 1)
        if row[0] not in mds_allowed_auth_plugins:
            if advices:
                output += util.print_red("{} is not supported in MDS".format(row[0]))

    return output

def get_users_privileges(session, advices=False):
    output = ""
    output_err = ""
    bad_users = 0
    sw_user = False
    title = "MDS Incompatible Privileges"
    # get all users
    stmt = "select user, host from mysql.user"
    result = session.run_sql(stmt)
    for row in result.fetch_all():
        stmt = "show grants for `{}`@`{}`".format(row[0], row[1])
        result2 = session.run_sql(stmt)
        sw_user = True
        for row2 in result2.fetch_all():
            m = re.match('GRANT (.*) ON', row2[0])
            if m:
                tab_grants = m.group(1).split(',')
                for priv in tab_grants:
                    if priv.strip() not in mds_allowed_privileges:
                        if sw_user:
                            bad_users += 1
                            sw_user = False
                        output_err += util.print_red("{}@{} has a privilege not supported in MDS: {}".format(row[0], row[1], priv.strip()))

    output += util.output(title, "{} user(s)".format(bad_users))
    if advices:
        output += output_err 
                        
    return output

def get_routines(session, advices=False, details=False):
    title   = "User Defined Routines"
    stmt    = "select routine_schema, count(*) `amount` from information_schema.routines where definer not in ('mysql.sys@localhost') group by routine_schema"
    result = session.run_sql(stmt)
    output = util.output(title, "")
    for row in result.fetch_all():
        output += util.output(row[0], row[1], 1)
        if advices and row[0] in "mysql":
            output += util.print_red("Custom routines in {} schema are not supported in MDS".format(row[0]))
        if details:
            stmt = """select routine_schema, routine_name, definer from information_schema.routines 
                      where routine_schema = '{}' and definer not in ('mysql.sys@localhost')""".format(row[0])
            output2, nbrows = util.run_and_print("Routines in {}".format(row[0]), stmt, session)
            output += output2


    return output

def get_tables_in_mysql(session, branch, advices=False, details=False):
    title   = "Extra tables in mysql schema"
    stmt = "select count(*) from information_schema.tables where table_schema='mysql'"
    output = ""
    tot_tbl = 28 
    if branch == "57": 
        tot_tbl = 31
    if branch == "80":
        tot_tbl = 38
    result = session.run_sql(stmt)
    row = result.fetch_one()
    if row[0] > tot_tbl:
        output = util.output(title, int(row[0])-tot_tbl)
        if advices:
            output += util.print_red("Extra tables in mysql schema are not supported in MDS")
        # TODO add which one 
    return output
        

def _get_type_by_pid(session):
    output = ""
    stmt = "select @@pid_file"
    result = session.run_sql(stmt)
    row = result.fetch_one()
    if row[0].startswith("/rdsdbdata/"):
        output = "(AWS RDS)"
    return output
