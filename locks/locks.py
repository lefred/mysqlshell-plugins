# locks/locks.py
# -----------------
# Definition of functions for the check and display locking info
#

from mysqlsh.plugin_manager import plugin, plugin_function
import struct

@plugin_function("locks.getLocks")
def show_locks(limit=10, session=None):
    """
    Prints the locks held by threads.

    This function list all locks held by a specific thread.

    Args:
        limit (integer): The amount of query to return (default: 10).
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

    stmt = """SELECT thr.processlist_id AS mysql_thread_id,
              FORMAT_PICO_TIME(trx.timer_wait) AS trx_duration,
              format_pico_time(sum(cpu_latency)) as cpu_latency,  format_bytes(sum(current_memory)) memory,
              COUNT(case when lock_status='GRANTED' then 1 else null end) AS row_locks_held,
              COUNT(case when lock_status='PENDING' then 1 else null end) AS row_locks_pending,
              GROUP_CONCAT(DISTINCT CONCAT(object_schema, '.', object_name)) AS tables_with_locks,
              sys.format_statement(current_statement) as current_statement
              FROM performance_schema.events_transactions_current trx
              INNER JOIN performance_schema.threads thr USING (thread_id)
              LEFT JOIN performance_schema.data_locks USING (thread_id)
              LEFT JOIN sys.x$processlist p on p.thd_id=thread_id
              WHERE thr.processlist_id IS NOT NULL
              GROUP BY thread_id, timer_wait ORDER BY TIMER_WAIT DESC
              LIMIT %d""" % (limit)

    result = session.run_sql(stmt)
    columns = result.get_column_names()
    rows = result.fetch_all()
    max_length=[]
    for i in range(8):
      if len(columns[i]) > max(len(str(x[i])) for x in rows):
          max_length.append(len(columns[i]))
      else:
          max_length.append(max(len(str(x[i])) for x in rows))

    line = "+-" + max_length[0]*"-" + "-+-" + max_length[1]*"-" + "-+-" + \
           max_length[2]*"-" + "-+-" + max_length[3]*"-" + "-+-" + max_length[4]*"-" + \
           "-+-" + max_length[5]*"-" + "-+-" + max_length[6]*"-" + "-+-" + max_length[7]*"-" + "-+"

    print(line)
    print("| {:{}} | {:{}} | {:{}} | {:{}} | {:{}} | {:{}} | {:{}} | {:{}} |".\
            format(columns[0], max_length[0],\
                columns[1], max_length[1],\
                columns[2], max_length[2],\
                columns[3], max_length[3],\
                columns[4], max_length[4],\
                columns[5], max_length[5],\
                columns[6], max_length[6],\
                columns[7], max_length[7]))

    print(line)
    events=[]
    for row in rows:
        events.append(row[0])
        print("| {:{}} | {:{}} | {:{}} | {:{}} | {:{}} | {:{}} | {:{}} | {:{}} |".\
            format(row[0], max_length[0],\
                row[1], max_length[1],\
                str(row[2] or ' '), max_length[2],\
                row[3], max_length[3],\
                row[4], max_length[4],\
                row[5], max_length[5],\
                str(row[6] or 'NULL'), max_length[6],\
                str(row[7] or 'NULL'), max_length[7]))
    print(line)
    answer = shell.prompt("For which thread_id do you want to see locks ? (%s) " %  events[0]
                               , {'defaultValue': str(events[0])})
    if int(answer) in events:
        print("Metadata Locks:")
        print("---------------")
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
            mdl_lock_summary.lock_summary
            FROM sys.processlist ps
            INNER JOIN mdl_lock_summary ON ps.thd_id=mdl_lock_summary.owner_thread_id
            WHERE conn_id=%s""" % answer
        result = session.run_sql(stmt)
        rows = result.fetch_all()
        if len(rows) > 0:
            print(rows[0][0])
        else:
            print("None")
        print("")
        print("Data Locks:")
        print("-----------")
        stmt = """SELECT OBJECT_SCHEMA, OBJECT_NAME, LOCK_TYPE,
                         LOCK_MODE, LOCK_STATUS, INDEX_NAME, GROUP_CONCAT(LOCK_DATA SEPARATOR '|')
                         FROM INFORMATION_SCHEMA.INNODB_TRX
                         JOIN performance_schema.data_locks d
                           ON d.ENGINE_TRANSACTION_ID = trx_id
                         WHERE trx_mysql_thread_id = %s GROUP BY 1,2,3,4,5,6 ORDER BY 1,2, 3 DESC, 6""" % answer
        result = session.run_sql(stmt)
        rows = result.fetch_all()
        prev_index=''
        if len(rows) > 0:
            for row in rows:
                if row[5] is None:
                    print("{} {} ({}) LOCK ON {}.{}".format(row[4], row[2], row[3], row[0], row[1]))
                else:
                    to_print = "{} {} ({}) LOCK ON {}.{} ({}) ".format(row[4], row[2], row[3], row[0], row[1], row[5])
                    str_len=len(to_print)
                    print(to_print, end='')
                    if row[5] != prev_index:
                        cols=[]
                        pk_cols=[]
                        stmt = """SELECT ifi.name, ifi.pos, ii.name, ifi.index_id
                                    FROM INFORMATION_SCHEMA.INNODB_TABLES it
                                    LEFT JOIN INFORMATION_SCHEMA.INNODB_INDEXES ii
                                           ON ii.table_id = it.table_id AND
                                              (ii.name = '{}' OR ii.name='PRIMARY')
                                    LEFT JOIN INFORMATION_SCHEMA.INNODB_FIELDS ifi
                                            ON ifi.index_id = ii.index_id
                                    WHERE it.name = '{}/{}'
                                    ORDER BY ii.NAME, POS""".format(row[5], row[0], row[1])
                        prev_index=row[5]
                        result2 = session.run_sql(stmt)
                        rows2 = result2.fetch_all()
                        for row2 in rows2:
                            if row2[2] == "PRIMARY":
                                pk_cols.append(row2[0])
                            else:
                                cols.append(row2[0])
                    # if there is an index name
                    if row[5] is not None:
                        records=row[6].split("|")
                        next_line=False
                        for record in records:
                            if next_line:
                               print(" " * str_len, end='')
                            # display columns and values
                            columns = record.split(', ')
                            if row[5] != "PRIMARY":
                                i=0
                                print("(", end='')
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
                                        print("{}={}{}".format(cols[i], column_to_disp, comma_str), end='')
                                    i+=1
                                print(") => ({}={})".format(pk_cols[0], columns[i-1]))
                            else:
                                print("(",end='')
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
                                    if (i < len(pk_cols)):
                                        if len(columns) < len(pk_cols):
                                            print("<", end='')
                                            j=0
                                            for pk_el in pk_cols:
                                                if j < (len(pk_cols))-1:
                                                    comma_str=", "
                                                else:
                                                    comma_str=""
                                                print("{}{}".format(pk_el, comma_str), end='')
                                                j+=1
                                            print(">=", end="")
                                            print(column, end="")
                                        else:
                                            print("{}={}{}".format(pk_cols[i], column, comma_str), end='')
                                    i+=1
                                print(")")

                            next_line=True
        else:
            print("None")
        # STAEMENTS BLOCKING US
        stmt = """SELECT REPLACE(locked_table,'`','') `TABLE`, locked_type, PROCESSLIST_INFO, waiting_lock_mode,
                         waiting_trx_rows_locked, waiting_trx_started, wait_age_secs, blocking_pid, last_statement
                  FROM performance_schema.threads AS t
                  JOIN sys.innodb_lock_waits AS ilw
                    ON ilw.waiting_pid = t.PROCESSLIST_ID 
                  JOIN sys.processlist proc 
                    ON proc.conn_id = blocking_pid
                  WHERE waiting_pid={}""".format(answer)
        result = session.run_sql(stmt)
        rows = result.fetch_all()
        for row in rows:
            print("\nBLOCKED FOR {} SECONDS BY (mysql_thread_id: {})".format(row[6], row[7]))
            print("\nLast statement of the blocking trx:")
            print("-----------------------------------")
            print("\033[31m{}\033[0m\n".format(row[8]))
        # STATEMENTS WE ARE BLOCKING
        stmt = """SELECT REPLACE(locked_table,'`','') `TABLE`, locked_type, waiting_query, waiting_lock_mode,
                         waiting_trx_rows_locked, waiting_trx_started, wait_age_secs, processlist_id
                  FROM performance_schema.threads AS t
                  JOIN sys.innodb_lock_waits AS ilw
                    ON ilw.waiting_pid = t.PROCESSLIST_ID where blocking_pid={}""".format(answer)
        result = session.run_sql(stmt)
        rows = result.fetch_all()
        for row in rows:
            print("\nBLOCKING {} ({}) LOCK ON {} FOR {} SECONDS (mysql_thread_id: {})".format(row[1], row[3], row[0], row[6], row[7]))
            print("\nStatement we are blocking:")
            print("--------------------------")
            print("\033[33m{}\033[0m\n".format(row[2]))


    else:
        print("%s is not part of the mysql_thread_id returned or is not valid!" % answer)

    return
