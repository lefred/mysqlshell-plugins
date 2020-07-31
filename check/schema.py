from ext.mysqlsh_plugins_common import run_and_show

def get_noninnodb_tables(session=None):

    stmt = """SELECT table_schema, table_name, engine, table_rows, 
                     sys.format_bytes(index_length+data_length) AS 'SIZE'
              FROM information_schema.tables 
              WHERE engine != 'innodb' 
              AND table_schema NOT IN 
               ('information_schema', 'mysql', 'performance_schema');"""
    
    run_and_show(stmt, "table", session)

def get_innodb_with_nopk(session=None):

    stmt = """SELECT tables.table_schema , tables.table_name , tables.engine,
                     table_rows, sys.format_bytes(index_length+data_length) AS 'SIZE' 
              FROM information_schema.tables 
              LEFT JOIN ( 
                    SELECT table_schema , table_name 
                    FROM information_schema.statistics 
                    GROUP BY table_schema, table_name, index_name HAVING 
                        SUM( case when non_unique = 0 and nullable != 'YES' then 1 else 0 end ) = count(*) ) puks 
              ON tables.table_schema = puks.table_schema and tables.table_name = puks.table_name 
              WHERE puks.table_name is null 
              AND tables.table_type = 'BASE TABLE' AND Engine="InnoDB";"""

    run_and_show(stmt, "table", session)

def get_cascading_fk(session=None):

    stmt = """SELECT CONCAT(t1.table_name, '.', column_name) AS 'foreign key',     
                     CONCAT(t1.referenced_table_name, '.', referenced_column_name) AS 'references',
                     t1.constraint_name AS 'constraint name', UPDATE_RULE, DELETE_RULE 
              FROM information_schema.key_column_usage as t1 
              JOIN information_schema.REFERENTIAL_CONSTRAINTS as t2 
              WHERE t2.CONSTRAINT_NAME = t1.constraint_name 
               AND t1.referenced_table_name IS NOT NULL 
               AND (DELETE_RULE = "CASCADE" OR UPDATE_RULE = "CASCADE");"""

    run_and_show(stmt, "table", session)
