import mysqlsh
import json
from mysqlsh.plugin_manager import plugin, plugin_function
from mysqlsh_plugins_common import run_and_show

def _is_qep_db_existing(session):
   stmt = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME='dba'"
   result = session.run_sql(stmt)
   row = result.fetch_all()
   if len(row) > 0:
       return True
   return False

def _is_qep_table_existing(session):
   stmt = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='dba' AND TABLE_NAME='qep'"
   result = session.run_sql(stmt)
   row = result.fetch_all()
   if len(row) > 0:
       return True
   return False


def _chek_dba_db(session, dbok):
    shell = mysqlsh.globals.shell
    # check if the database 'dba' exists
    create_table_stmt = """CREATE TABLE dba.qep (
  `id` int NOT NULL AUTO_INCREMENT,
  `common_hash` varchar(56) DEFAULT NULL,
  `inserted` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `qep` json DEFAULT NULL,
  `query_cost` float GENERATED ALWAYS AS (json_unquote(json_extract(`qep`,_utf8mb4'$."query_block"."cost_info"."query_cost"'))) VIRTUAL,
  `qep_tree` text,
  `version` varchar(20),
  PRIMARY KEY (`id`),
  KEY `hash_idx` (`common_hash`)
) ENGINE=InnoDB"""
    if not dbok:
        if _is_qep_db_existing(session):
            if _is_qep_table_existing(session):
                dbok = True
            else:
                answer = shell.prompt("The table 'qep' is missing in schema 'dba', do you wanna create it? (y/N) ", {'defaultValue':'n'})
                if answer.lower() == 'y':
                    try:
                        session.run_sql(create_table_stmt)
                    except:
                        print("ERROR: problem creating 'qep' table.")
                        return dbok
        else:
            answer = shell.prompt("The schema 'dba' is missing, do you wanna create it? (y/N) ", {'defaultValue':'n'})
            if answer.lower() == 'y':
               query = """CREATE DATABASE dba"""
               try:
                    session.run_sql(query)
                    try:
                        session.run_sql(create_table_stmt)
                    except:
                        print("ERROR: problem creating 'qep' table.")
                        return dbok
               except:
                   print("ERROR: problem creating 'dba' schema.")
                   return dbok

    return True


def _save_qep(stmt, qep, qep_tree, session, dbok):
    dbok = _chek_dba_db(session, dbok)
    # save the json qep
    query = """INSERT INTO dba.qep(common_hash, qep, qep_tree, version)
               VALUES (SHA2(TRIM(statement_digest_text(?)), 224),
                ?, ?, @@version);"""
    session.run_sql(query, [stmt, qep, qep_tree])
    return dbok

def _load_qep(stmt, qep, session, dbok):
    dbok = _chek_dba_db(session, dbok)

    query = """SELECT inserted, query_cost FROM dba.qep WHERE common_hash=SHA2(TRIM(statement_digest_text(?)), 224)
               ORDER BY inserted DESC LIMIT 1"""
    result = session.run_sql(query, [stmt])
    row = result.fetch_one()
    got_one = False
    if row:
        print("The last QEP saved for this query on {} had a cost of {}".format(row[0], row[1]))
        got_one = True
    return dbok, got_one

def _load_qep_all(stmt, session):
    query = """SELECT id, inserted, query_cost, version FROM dba.qep WHERE common_hash=SHA2(TRIM(statement_digest_text(?)), 224)
               ORDER BY inserted DESC"""
    result = session.run_sql(query, [stmt])
    rows = result.fetch_all()
    ids=[]
    for row in rows:
        rec=[]
        rec.append(row[0])
        rec.append(row[1])
        rec.append(row[2])
        rec.append(row[3])
        ids.append(rec)

    return ids

def _get_qep_by_id(id, session):
    query = """SELECT qep, qep_tree FROM dba.qep WHERE id=?"""
    result = session.run_sql(query, [id])
    row = result.fetch_one()
    return(row[0], row[1])

def get_full_detail(original_query, session, dbok):
    shell = mysqlsh.globals.shell
    stmt = """EXPLAIN FORMAT=json %s""" % original_query
    try:
        result = session.run_sql(stmt)
    except mysqlsh.DBError as err:
        print("Aborting: {}".format(err))
        return

    row = result.fetch_one()
    qep=row[0]
    qep_json = json.loads(qep)
    print("The cost of the query is {}".format(qep_json["query_block"]["cost_info"]["query_cost"]))
    answer = shell.prompt('Do you want to have EXPLAIN output? (y/N) ', {'defaultValue':'n'})
    if answer.lower() == 'y':
        stmt = """EXPLAIN %s""" % original_query
        run_and_show(stmt,'vertical')
    answer = shell.prompt('Do you want to have EXPLAIN in JSON format output? (y/N) ', {'defaultValue':'n'})
    if answer.lower() == 'y':
        print(qep)
    stmt = """EXPLAIN format=tree %s""" % original_query
    result = session.run_sql(stmt)
    row = result.fetch_one()
    qep_tree=row[0]
    answer = shell.prompt('Do you want to have EXPLAIN in TREE format output? (y/N) ', {'defaultValue':'n'})
    if answer.lower() == 'y':
        print(qep_tree)
    answer = shell.prompt('Do you want to have EXPLAIN ANALYZE output? (y/N) ', {'defaultValue':'n'})
    if answer.lower() == 'y':
        stmt = """EXPLAIN ANALYZE %s""" % original_query
        result = session.run_sql(stmt)
        row = result.fetch_one()
        print(row[0])

    dbok, got_one = _load_qep(original_query, qep, session, dbok)

    if dbok and got_one:
        answer = shell.prompt('Do you want to compare with a previous QEP? (y/N) ', {'defaultValue':'n'})
        if answer.lower() == 'y':
            all_ids = _load_qep_all(original_query,session)
            i = 0
            if len(all_ids)>0:
                fmt = "| {0:>3s} | {1:20s} | {2:>11s} | {3:>18s} |"
                header = fmt.format("Num", "Timestamp", "Query Cost", "Version")
                bar = "+" + "-" * 5 + "+" + "-" * 22 + "+" + "-" * 13 + "+" + "-" * 20 + "+"
                print (bar)
                print (header)
                print (bar)
                for rec in all_ids:
                    i+=1
                    print(fmt.format(str(i), str(rec[1]), str(rec[2]),str(rec[3]) ))
                print (bar)

                while True:
                    answer = shell.prompt('With which previous QEP do you want to compare? (1) ', {'defaultValue':'1'})
                    if answer.isdigit():
                        if int(answer) >0 and int(answer) <= len(all_ids):
                            qep2, qep2_tree = _get_qep_by_id(all_ids[int(answer)-1][0], session)
                            print("\033[33mCURRENT:\n--------\n{}\033[0m".format(qep_tree))
                            print("\033[36mPREVIOUS:\n---------\n{}\033[0m".format(qep2_tree))
                            print()
                            break


    answer = shell.prompt('Do you want to save the QEP? (y/N) ', {'defaultValue':'n'})
    if answer.lower() == 'y':
        _save_qep(original_query, qep, qep_tree, session, dbok)



    return dbok
