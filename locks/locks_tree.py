# locks/locks_tree.py
# -----------------
# Definition of functions for the check and display locking info
#

from mysqlsh.plugin_manager import plugin, plugin_function
from support.sections import util
import struct
   
@plugin_function("locks.getAllLocks")
def show_locks(timeout=1, session=None):
    """
    Prints the locks held by threads.

    This function list all locks held by a specific thread.

    Args:
        timeout (integer): The timeout in seconds to retrieve extra locking info (default: 1).
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

    try:
        from prettytable import PrettyTable
    except:
        return(util.print_red("Error importing module prettytable, check if it's installed (ex: mysqlsh --pym pip install --user prettytable)"))
    
    # Get all locks

    stmt = """WITH mdl_lock_summary AS (
            SELECT
            owner_thread_id,
            GROUP_CONCAT(
            DISTINCT
            CONCAT(
            LOCK_STATUS, ' ',
            lock_type, ' on ',
            IF(object_type='USER LEVEL LOCK', CONCAT(object_name, ' (user lock)'), CONCAT(OBJECT_SCHEMA, '.', OBJECT_NAME))
            )
            ORDER BY object_type ASC, LOCK_STATUS ASC, lock_type ASC
            SEPARATOR '\n'
            ) AS lock_summary
            FROM performance_schema.metadata_locks
            GROUP BY owner_thread_id
            )
            SELECT 
            thd_id, conn_id, user, db, command,  time as sec_state, trx_latency, trx_state, lock_latency, 
            ifnull(last_statement, current_statement) "statement (last or current)", lock_summary             
            FROM sys.processlist ps
            INNER JOIN mdl_lock_summary ON ps.thd_id=mdl_lock_summary.owner_thread_id where conn_id != @@pseudo_thread_id
            """

    result = session.run_sql(stmt)
    columns = result.get_column_names()
    headers=["thd_id", "conn_id", "blocking", "blocked by","user", "db", "command", "sec_state", "trx_latency", "trx_state", "lock_latency",
             "statement (last/current)", "lock_summary"]

    tab = PrettyTable(headers)
    tab.align = 'r'
    all_rows=[]
    
    for record in result.fetch_all():
        got_waiting = False
        # get eventual thread we are blocking
        stmt = """SELECT THREAD_ID, waiting_lock_mode
                  FROM performance_schema.threads AS t
                  JOIN sys.innodb_lock_waits AS ilw
                    ON ilw.waiting_pid = t.PROCESSLIST_ID where blocking_pid={}""".format(record[1])
        result_block = session.run_sql(stmt)
        thread_blocked="{"
        for blocked in result_block.fetch_all():
            thread_blocked+="{},".format(blocked[0])
        if len(thread_blocked)>1:
            thread_blocked = thread_blocked[:-1]
        thread_blocked += "}"
        
        # get eventual thread blocking us
        stmt = """SELECT thd_id FROM performance_schema.threads AS t
         JOIN sys.innodb_lock_waits AS ilw
           ON ilw.waiting_pid = t.PROCESSLIST_ID
         JOIN sys.processlist proc
           ON proc.conn_id = blocking_pid 
           WHERE thread_id = {}""".format(record[0])
        result_block = session.run_sql(stmt)
        thread_blocking="{"
        for blocked in result_block.fetch_all():
            thread_blocking+="{},".format(blocked[0])
        if len(thread_blocking)>1:
            thread_blocking = thread_blocking[:-1]
        thread_blocking += "}"
        
        row=[record[0], record[1], thread_blocked, thread_blocking, record[2],
             record[3], record[4], record[5], 
             record[6], record[7], record[8],
             record[9], "{} (metadata lock)".format(record[10])
        ]
        all_rows.append(row)
        record_row=len(all_rows)-1
        # get now specific recs lock info
        stmt = """SELECT OBJECT_SCHEMA, OBJECT_NAME, LOCK_TYPE,
                        LOCK_MODE, LOCK_STATUS, INDEX_NAME, GROUP_CONCAT(LOCK_DATA SEPARATOR '|') recs
                        FROM INFORMATION_SCHEMA.INNODB_TRX
                        JOIN performance_schema.data_locks d
                        ON d.ENGINE_TRANSACTION_ID = trx_id
                        WHERE trx_mysql_thread_id = {} GROUP BY 1,2,3,4,5,6 ORDER BY 1,2, 3 DESC, 6
        """.format(record[1])
        result2 = session.run_sql(stmt)
        prev_index=''
        for record2 in result2.fetch_all():
            if record2[4] == "WAITING":
                got_waiting = True
            row=["","","","","","","","","","","", ""]
            if record2[5] is None:
                row_to_append="{} {} ({}) LOCK ON {}.{}".format(record2[4], record2[2], record2[3], record2[0], record2[1])
            else:
                row_to_append="{} {} ({}) LOCK ON {}.{} ({})".format(record2[4], record2[2], record2[3], record2[0], record2[1], record2[5])

            if record2[5] != prev_index:
                cols=[]
                pk_cols=[]
                stmt = """SELECT /*+ MAX_EXECUTION_TIME({}) */ ifi.name, ifi.pos, ii.name, ifi.index_id
                            FROM INFORMATION_SCHEMA.INNODB_TABLES it
                            LEFT JOIN INFORMATION_SCHEMA.INNODB_INDEXES ii
                                    ON ii.table_id = it.table_id AND
                                        (ii.name = '{}' OR ii.name='PRIMARY')
                            LEFT JOIN INFORMATION_SCHEMA.INNODB_FIELDS ifi
                                    ON ifi.index_id = ii.index_id
                            WHERE it.name = '{}/{}'
                            ORDER BY ii.NAME, POS""".format(timeout*1000, record2[5], record2[0], record2[1])
                prev_index=record2[5]
                skip=False
                try:
                    result3 = session.run_sql(stmt)
                    records3 = result3.fetch_all()
                    for record3 in records3:
                        if record3[2] == "PRIMARY":
                            pk_cols.append(record3[0])
                        else:
                            cols.append(record3[0])
                except:
                    skip=True
                
            # if there is an index name
            if record2[5] is not None and record2[6] is not None:
                if "|" in record2[6]:
                    records3=record2[6].split("|")
                else:
                    records3=[record2[6]]
                next_line=False
                j = 0
                for record3 in records3:
                    # display columns and values
                    columns = record3.split(', ')
                    if record2[5] != "PRIMARY":
                        i=0
                        row_to_append += "("
                        for column in columns:
                            if len(column) == 10 and str(column).startswith("0x"):
                                column_to_disp = int(struct.unpack('f', struct.pack('>l', int(column, 0)))[0])
                            else:
                                column_to_disp = column
                            if i < len(columns)-2:
                                comma_str=", "
                            else:
                                comma_str=""
                            if (i < len(cols)):
                                row_to_append += "{}={}{}".format(cols[i], column_to_disp, comma_str)
                            i+=1
                        if not skip: 
                            row_to_append += ") => ({}={})".format(pk_cols[0], columns[i-1])
                        else:
                            row_to_append += "[col?]={}) => ([pk]={})".format(columns[0],column_to_disp )
                        j += 1
                        if len(records3) > 1 and j < len(records3):
                            row_to_append += "\n"
                    else:
                        row_to_append += "("
                        i=0
                        for column in columns:
                            if len(column) == 10 and str(column).startswith("0x"):
                                column_to_disp = int(struct.unpack('f', struct.pack('>l', int(column, 0)))[0])
                            else:
                                column_to_disp = column
                            if i < (len(columns))-1:
                                comma_str=", "
                            else:
                                comma_str=""
                            if not skip: 
                                if (i < len(pk_cols)):
                                    if len(columns) < len(pk_cols):
                                        row_to_append += "<"
                                        j=0
                                        for pk_el in pk_cols:
                                            if j < (len(pk_cols))-1:
                                                comma_str=", "
                                            else:
                                                comma_str=""
                                            row_to_append += "{}{}".format(pk_el, comma_str)
                                            j+=1
                                        row_to_append += ">="
                                        row_to_append += column
                                    else:
                                        row_to_append += "{}={}{}".format(pk_cols[i], column, comma_str)
                            else:
                                row_to_append += "[col?]={}{}".format( column, comma_str)
                            i+=1
                        row_to_append += ")"
                        j += 1
                        if len(records3) > 1 and j < len(records3):
                            row_to_append += "\n"

            row.append(row_to_append)
            all_rows.append(row)

        if not got_waiting and len(all_rows[record_row][3]) >2:
            all_rows[record_row][3]="{}<?>".format(all_rows[record_row][3])
            # let's try to get the thread id: 
            no_more_blocked_thd = all_rows[record_row][0]

    for row in all_rows:
        tab.add_row(row)

    print(str(tab))