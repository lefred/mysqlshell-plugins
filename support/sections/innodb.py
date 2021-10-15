from support.sections import util


def _get_ahi_details(session, advices):
    title = "Adaptive Hash Index"
    stmt = """SELECT ROUND(
            (
              SELECT Variable_value FROM sys.metrics
              WHERE Variable_name = 'adaptive_hash_searches'
            ) /
            (
              (
               SELECT Variable_value FROM sys.metrics
               WHERE Variable_name = 'adaptive_hash_searches_btree'
              )  + (
               SELECT Variable_value FROM sys.metrics
               WHERE Variable_name = 'adaptive_hash_searches'
              )
            ) * 100,2
          ) 'AHIRatio',
		  ROUND(
            (
              SELECT Variable_value FROM sys.metrics
              WHERE Variable_name = 'adaptive_hash_searches'
            ) /
            (
              (
               SELECT Variable_value FROM sys.metrics
               WHERE Variable_name = 'adaptive_hash_searches_btree'
              )  + (
               SELECT Variable_value FROM sys.metrics
               WHERE Variable_name = 'adaptive_hash_searches'
              )
            ) * 100
          ) 'AHIRatioInt',
		  (
					SELECT variable_value
					FROM performance_schema.global_variables
					WHERE variable_name = 'innodb_adaptive_hash_index'
		  ) AHIEnabled,
		  (
					SELECT variable_value
					FROM performance_schema.global_variables
					WHERE variable_name = 'innodb_adaptive_hash_index_parts'
		  ) AHIParts"""
    result = session.run_sql(stmt)
    row = result.fetch_one()
    output = util.output(title,"")
    output += util.output("AHI Enabled", row[2], 1)
    output += util.output("AHI Parts", row[3], 1)
    output += util.output("AHI Ratio", row[0], 1)
    
    if advices and row[2] == "ON":
        output += util.print_orange("AHI is not recommended for all workloads")
    return output

def _get_ahi_details_56(session, advices):
    title = "Adaptive Hash Index"
    stmt = "select @@innodb_adaptive_hash_index"
    result = session.run_sql(stmt)
    row = result.fetch_one()
    output = util.output(title, row[0])
    if advices and row[0] == 1:
        output += util.print_orange("AHI is not recommended for all workloads")
    return output


def _get_buffer_pool_details(session, branch):
    title = "InnoDB Buffer Pool"
    usesys = ""
    if branch == "57":
        usesys="sys."
    stmt = """SELECT ROUND(A.num * 100.0 / B.num)  BufferPoolFull, BP_Size, BP_instances,
FORMAT(F.num * 100.0 / E.num,2) DiskReadRatio, 
ROUND(F.num*100/E.num) DiskReadRatioInt
FROM (
	SELECT variable_value num FROM performance_schema.global_status
	WHERE variable_name = 'Innodb_buffer_pool_pages_data') A,
     (
	SELECT variable_value num FROM performance_schema.global_status
	WHERE variable_name = 'Innodb_buffer_pool_pages_total') B,
     (
	SELECT {}format_bytes(variable_value) as BP_Size
	FROM performance_schema.global_variables
	WHERE variable_name = 'innodb_buffer_pool_size') C,
     (
	SELECT variable_value as BP_instances
	FROM performance_schema.global_variables
	WHERE variable_name = 'innodb_buffer_pool_instances') D,
     (     
   	SELECT variable_value num 
	FROM performance_schema.global_status       
	WHERE variable_name = 'Innodb_buffer_pool_read_requests') E,       
     (
   	SELECT variable_value num 
	FROM performance_schema.global_status        
	WHERE variable_name = 'Innodb_buffer_pool_reads') F
    """.format(usesys) 
    result = session.run_sql(stmt)
    row = result.fetch_one()
    output = util.output(title,"")
    output += util.output("BP Size", row[1], 1)
    output += util.output("BP Instance(s)", row[2], 1)
    output += util.output("BP filled at ", "{}%".format(row[0]), 1)
    output += util.output("Disk Read Ratio", "{}%".format(row[3]), 1)

    return output

def _get_buffer_pool_details_56(session):
    title = "InnoDB Buffer Pool"
    stmt = """SELECT FORMAT(A.num * 100.0 / B.num,2) BufferPoolFullPct, 
        concat(round(@@innodb_buffer_pool_size/1024/1024)," MiB") BP_Size, 
        @@innodb_buffer_pool_instances BP_instances FROM
	(SELECT variable_value num FROM information_schema.global_status
	WHERE variable_name = 'Innodb_buffer_pool_pages_data') A,
	(SELECT variable_value num FROM information_schema.global_status
	WHERE variable_name = 'Innodb_buffer_pool_pages_total') B"""
    result = session.run_sql(stmt)
    row = result.fetch_one()
    output = util.output(title,"")
    output += util.output("BP Size", row[1], 1)
    output += util.output("BP Instance(s)", row[2], 1)
    output += util.output("BP filled at ", "{}%".format(row[0]), 1)

    return output

def _get_innodb_log_details(session):
    title = "InnoDB Logs"
    stmt = """SELECT CONCAT(
		            (
						SELECT FORMAT_BYTES(
							STORAGE_ENGINES->>'$."InnoDB"."LSN"' - STORAGE_ENGINES->>'$."InnoDB"."LSN_checkpoint"'
							               )
								FROM performance_schema.log_status),
						" / ",
						format_bytes(
							(SELECT VARIABLE_VALUE
								FROM performance_schema.global_variables
								WHERE VARIABLE_NAME = 'innodb_log_file_size'
							)  * (
							 SELECT VARIABLE_VALUE
							 FROM performance_schema.global_variables
							 WHERE VARIABLE_NAME = 'innodb_log_files_in_group'))
					) CheckpointInfo,
					(
						SELECT ROUND(((
							SELECT STORAGE_ENGINES->>'$."InnoDB"."LSN"' - STORAGE_ENGINES->>'$."InnoDB"."LSN_checkpoint"'
							FROM performance_schema.log_status) / ((
								SELECT VARIABLE_VALUE
								FROM performance_schema.global_variables
								WHERE VARIABLE_NAME = 'innodb_log_file_size'
							) * (
							SELECT VARIABLE_VALUE
							FROM performance_schema.global_variables
							WHERE VARIABLE_NAME = 'innodb_log_files_in_group')) * 100),2)
					)  AS CheckpointAge,
					format_bytes( (
						SELECT VARIABLE_VALUE
						FROM performance_schema.global_variables
						WHERE variable_name = 'innodb_log_file_size')
					) AS InnoDBLogFileSize,
					(
						SELECT VARIABLE_VALUE
						FROM performance_schema.global_variables
						WHERE variable_name = 'innodb_log_files_in_group'
					) AS NbFiles,
					(
						SELECT VARIABLE_VALUE
						FROM performance_schema.global_status
						WHERE VARIABLE_NAME = 'Innodb_redo_log_enabled'
					) AS RedoEnabled, 
					(
						SELECT variable_value
						FROM performance_schema.global_variables
						WHERE variable_name = 'innodb_flush_log_at_trx_commit'
					) AS FlushAtCommit
    """ 
    result = session.run_sql(stmt)
    row = result.fetch_one()
    output = util.output(title,"")
    output += util.output("File Size", row[2], 1)
    output += util.output("Nb of Files", row[3], 1)
    output += util.output("Checkpoint Info", row[0], 1)
    output += util.output("CheckPointAge", "{}%".format(row[1]), 1)
    output += util.output("Flush at Commit", row[5], 1)
    output += util.output("Redo Log", row[4], 1)

    return output

def _get_innodb_log_details_57(session):
    title = "InnoDB Logs"
    stmt = """SELECT sys.format_bytes(Log_File_Size) Log_File_Size, 
              sys.format_bytes(Log_File_Size * Nb_files) Tot_Log_Size, Nb_Files, Flush_at_trx_commit 
  FROM (	
	SELECT variable_value as Log_File_Size
	FROM performance_schema.global_variables
	WHERE variable_name = 'innodb_log_file_size') A,
     (
	SELECT variable_value as Flush_at_Trx_commit
	FROM performance_schema.global_variables
	WHERE variable_name = 'innodb_flush_log_at_trx_commit') B,
     (     
	SELECT variable_value as Nb_Files
	FROM performance_schema.global_variables
	WHERE variable_name = 'innodb_log_files_in_group') C
    """ 
    result = session.run_sql(stmt)
    row = result.fetch_one()
    output = util.output(title,"")
    output += util.output("File Size", row[0], 1)
    output += util.output("Nb of Files", row[2], 1)
    output += util.output("Total Size", row[1], 1)
    output += util.output("Flush at Commit", row[3], 1)

    return output

def _get_innodb_log_details_56(session):
    title = "InnoDB Logs"
    stmt = """SELECT concat(round(@@innodb_log_file_size/1024/1024), " MiB") Log_File_Size, 
              concat(round((@@innodb_log_file_size * @@innodb_log_files_in_group)/1024/1024), " MiB") Tot_Log_Size, 
              @@innodb_log_files_in_group Nb_files, @@innodb_flush_log_at_trx_commit Flush_at_trx_commit"""
    result = session.run_sql(stmt)
    row = result.fetch_one()
    output = util.output(title,"")
    output += util.output("File Size", row[0], 1)
    output += util.output("Nb of Files", row[2], 1)
    output += util.output("Total Size", row[1], 1)
    output += util.output("Flush at Commit", row[3], 1)

    return output

def get_innodb_info(session, advices, branch, releasever):
    output = ""
    if branch == "56":
        output =_get_buffer_pool_details_56(session)
        output += "\n"
        output += _get_innodb_log_details_56(session)
        output += "\n"
        output += _get_ahi_details_56(session, advices)
    else:
        output = _get_buffer_pool_details(session, branch)
        output += "\n"
        if branch == "80":
          output += _get_innodb_log_details(session)
        else:
          output += _get_innodb_log_details_57(session)
        output += "\n"
        output +=_get_ahi_details(session, advices)
    return output

    