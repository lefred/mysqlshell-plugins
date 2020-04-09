# check/locks.py
# -----------------
# Definition of functions for the check and display locking info 
#

def show_locks(limit=10, session=None):
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
              COUNT(case when lock_status='GRANTED' then 1 else null end) AS row_locks_held,
              COUNT(case when lock_status='PENDING' then 1 else null end) AS row_locks_pending,
              GROUP_CONCAT(DISTINCT CONCAT(object_schema, '.', object_name)) AS tables_with_locks,
              current_statement 
              FROM performance_schema.events_transactions_current trx 
              INNER JOIN performance_schema.threads thr USING (thread_id) 
              LEFT JOIN performance_schema.data_locks USING (thread_id) 
              LEFT JOIN sys.processlist p on p.thd_id=thread_id 
              WHERE thr.processlist_id IS NOT NULL 
              GROUP BY thread_id, timer_wait ORDER BY TIMER_WAIT DESC
              LIMIT %d""" % (limit)
            
    result = session.run_sql(stmt)
    columns = result.get_column_names()
    rows = result.fetch_all()
    max_length=[]
    for i in range(6): 
      if len(columns[i]) > max(len(str(x[i])) for x in rows):
          max_length.append(len(columns[i]))
      else:
          max_length.append(max(len(str(x[i])) for x in rows))
    
    line = "+-" + max_length[0]*"-" + "-+-" + max_length[1]*"-" + "-+-" + \
           max_length[2]*"-" + "-+-" + max_length[3]*"-" + "-+-" + max_length[4]*"-" + \
           "-+-" + max_length[5]*"-" + "-+" 

    print(line)
    print("| {:{}} | {:{}} | {:{}} | {:{}} | {:{}} | {:{}} |".\
            format(columns[0], max_length[0],\
                columns[1], max_length[1],\
                columns[2], max_length[2],\
                columns[3], max_length[3],\
                columns[4], max_length[4],\
                columns[5], max_length[5]))

    print(line)
    events=[]
    for row in rows:
        events.append(row[0])
        print("| {:{}} | {:{}} | {:{}} | {:{}} | {:{}} | {:{}} |".\
            format(row[0], max_length[0],\
                row[1], max_length[1],\
                row[2], max_length[2],\
                row[3], max_length[3],\
                str(row[4] or 'NULL'), max_length[4],\
                str(row[5] or 'NULL'), max_length[5]))
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
        stmt = """SELECT CONCAT(OBJECT_SCHEMA,'.', OBJECT_NAME) as `TABLE`, LOCK_TYPE, 
                         LOCK_MODE, LOCK_STATUS FROM sys.processlist p 
                         JOIN performance_schema.data_locks d 
                         ON d.thread_id = p.thd_id
                         WHERE conn_id = %s""" % answer
        result = session.run_sql(stmt)
        rows = result.fetch_all()
        if len(rows) > 0:
            for row in rows:
                print("{} {} ({}) LOCK on {}".format(row[3], row[1], row[2], row[0])) 
        else:
            print("None")

    else:
        print("%s is not part of the mysql_thread_id returned or is not valid!" % answer)

    return