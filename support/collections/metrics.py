from numpy import size
import support.collections.common as common
common.collectList.append("metrics.collect")
common.plotList.append("metrics.plot")

def collect(session, header, minute_cpt):
    stmt = "select unix_timestamp() as `timestamp`, t1.* from sys.metrics as t1"
    common._run_me(session, stmt, header, "metrics.txt")
    return

def plot():
    import pandas as pd
    data = pd.read_csv("{}/metrics.txt".format(common.outdir), sep='\t')
     
    # innodb_log_writes vs innodb_log_write_requests
    common._generate_graph("innodb_log.png", "InnoDB Log", data, [["innodb_log_writes"], ["innodb_log_write_requests"]])
    # innodb_buffer_pool_reads vs innodb_buffer_pool_read_requests
    common._generate_graph("innodb_reads.png", "InnoDB Reads", data, [["innodb_buffer_pool_reads"], ["innodb_buffer_pool_read_requests"]])
    # InnoDB Flushing (innodb_buffer_pool_pages_flushed)
    common._generate_graph("innodb_flushing.png", "InnoDB Flushing", data, [["innodb_buffer_pool_pages_flushed"]])
    # InnoDB OS Log Writes (innodb_os_log_written)
    common._generate_graph("innodb_os_log.png", "InnoDB OS Log Writes", data, [["innodb_os_log_written"]])
    # InnoDB Pending
    common._generate_graph("innodb_pending.png", "InnoDB Pending", data, [["innodb_data_pending_fsyncs",1], 
                                                                          ["innodb_data_pending_reads",1],
                                                                          ["innodb_data_pending_writes",1]])
    # Threads
    common._generate_graph("mysql_threads.png", "MySQL Threads", data, [["threads_cached",1], 
                                                                        ["threads_connected",1],
                                                                        ["threads_created"],
                                                                        ["threads_running",1]], "line")
    # MySQL Threads Running
    common._generate_graph("mysql_threads_running.png", "MySQL Threads Running", data, [["threads_running",1]])

    # Aborted connections
    common._generate_graph("mysql_aborted_connections.png", "MySQL Aborted Connections", data, [["aborted_clients"], 
                                                                        ["aborted_connects"], 
                                                                        ["mysqlx_aborted_clients"]])
    
    # Joins
    common._generate_graph("mysql_joins.png", "MySQL Joins", data, [["select_full_join"], 
                                                                        ["select_full_range_join"],
                                                                        ["select_range"],
                                                                        ["select_range_check"],
                                                                        ["select_scan"]], "line")
    
    # Sorting
    common._generate_graph("mysql_sorts.png", "MySQL Sorting", data, [["sort_merge_passes"], 
                                                                        ["sort_range"],
                                                                        ["sort_rows"],
                                                                        ["sort_scan"]], "line")

    # Temporary Tables
    common._generate_graph("mysql_tmp_tables.png", "MySQL Temporary Tables", data, [["created_tmp_disk_tables"], 
                                                                        ["created_tmp_tables"]], "area")

    # Handler Statistics
    common._generate_graph("mysql_handler_stats.png", "MySQL Handler Statistics", data, [["handler_delete"], 
                                                                        ["handler_read_first"],
                                                                        ["handler_read_key"],
                                                                        ["handler_read_last"],
                                                                        ["handler_read_prev"],
                                                                        ["handler_read_next"],
                                                                        ["handler_read_rnd"],
                                                                        ["handler_read_rnd_next"],
                                                                        ["handler_update"],
                                                                        ["handler_write"]], "line")

    # MySQL Queries
    common._generate_graph("mysql_queries.png", "MySQL Queries", data, [["queries"]], "area")  

    # InnoDB Inserts
    common._generate_graph("mysql_dml.png", "MySQL DML", data, [["dml_deletes"], 
                                                                ["dml_inserts"], 
                                                                ["dml_reads"], 
                                                                ["dml_system_deletes"], 
                                                                ["dml_system_inserts"], 
                                                                ["dml_system_reads"], 
                                                                ["dml_system_updates"], 
                                                                ["dml_updates"]], "area") 

    # Buffer Pool
    common._generate_graph("innodb_buffer_pool.png", "MySQL InnoDB Buffer Pool", data, [  
                                                                        ["innodb_buffer_pool_pages_data", 1], 
                                                                        ["innodb_buffer_pool_pages_misc", 1],
                                                                        ["innodb_buffer_pool_pages_free", 1]], "area", True)

    # Buffer Pool Dirty
    common._generate_graph("innodb_buffer_pool_dirty.png", "MySQL InnoDB Buffer Pool - Dirty Pages", data, [  
                                                                        ["innodb_buffer_pool_pages_data", 1], 
                                                                        ["innodb_buffer_pool_pages_dirty", 1]], "area", False)


    # Checkpoint Age
    common._generate_graph("mysql_checkpoint.png", "MySQL Checkpoint Age", data, [["log_lsn_checkpoint_age",1]], "area")  

    return
