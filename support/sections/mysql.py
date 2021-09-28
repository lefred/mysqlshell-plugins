from support.sections import util


def version_info(session):
    supported = False
    stmt = "select @@version_comment, @@version, @@version_compile_machine"
    result = session.run_sql(stmt)
    row = result.fetch_one()
    output = util.output("MySQL Version", "%s (%s) - %s" % (row[0], row[1], row[2]))
    if int(row[1][0]) >= 8 and row[0].startswith('MySQL'):
        supported = True
    return supported, output

def get_dataset(session):
    stmt = """SELECT format_bytes(SUM(data_length)) Data,
                     format_bytes(SUM(index_length)) Indexes,
                     format_bytes(SUM(data_length)+sum(index_length)) 'Total Size'
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



def get_largest_innodb_tables(session, limit=10):
    try:
        from prettytable import PrettyTable
    except:
        return(util.print_red("Error importing module prettytable, check if it's installed"))

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
    result = session.run_sql(stmt)
    headers=[]
    for col in result.get_columns():
        headers.append(col.get_column_label())

    tab = PrettyTable(headers)
    tab.align = 'r'

    output = util.output("Top %s largest InnoDB Tables" % str(limit), "")
    for row in result.fetch_all():
        tab.add_row(row)
    tab.align[result.get_columns()[0].get_column_label()] = 'l'
    output += str(tab) + "\n"
    return output

def get_tables_without_pk(session):
    try:
        from prettytable import PrettyTable
    except:
        return(util.print_red("Error importing module prettytable, check if it's installed"))

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
    headers=[]
    for col in result.get_columns():
        headers.append(col.get_column_label())

    tab = PrettyTable(headers)
    tab.align = 'r'

    output = util.output("Tables without PK", "")
    for row in result.fetch_all():
        tab.add_row(row)
    tab.align[result.get_columns()[0].get_column_label()] = 'l'
    output += str(tab) + "\n"
    return output
