from support.sections import util


def get_replication_info(session, advices, branch, releasever):
    supported = False
    if branch == "80":
        if int(releasever) > 19:
            stmt = """select @@server_id, @@binlog_checksum, @@binlog_encryption, @@binlog_format, @@binlog_row_image,
                     @@binlog_row_metadata, @@enforce_gtid_consistency, @@gtid_mode, @@log_bin, 
                     @@log_bin_basename, @@transaction_write_set_extraction, @@sync_binlog,
                     @@binlog_transaction_dependency_tracking, @@lower_case_table_names,  @@binlog_transaction_compression"""
        else:
            stmt = """select @@server_id, @@binlog_checksum, @@binlog_encryption, @@binlog_format, @@binlog_row_image,
                     @@binlog_row_metadata, @@enforce_gtid_consistency, @@gtid_mode, @@log_bin, 
                     @@log_bin_basename, @@transaction_write_set_extraction,  @@sync_binlog,
                     @@binlog_transaction_dependency_tracking, @@lower_case_table_names"""
    else:
        if branch == "56":
            stmt = """select @@server_id, @@binlog_checksum, "n/a", @@binlog_format, @@binlog_row_image,
                     "n/a", @@enforce_gtid_consistency, @@gtid_mode, @@log_bin,  @@sync_binlog,
                     @@log_bin_basename, "n/a", "n/a", @@lower_case_table_names"""
        else:
            if branch == "57":
                if int(releasever) > 21:
                    stmt = """select @@server_id, @@binlog_checksum, @@binlog_format, @@binlog_row_image,
                     @@enforce_gtid_consistency, @@gtid_mode, @@log_bin, 
                     @@log_bin_basename, @@transaction_write_set_extraction, @@sync_binlog,
                     @@binlog_transaction_dependency_tracking, @@lower_case_table_names"""
                else:
                    stmt = """select @@server_id, @@binlog_checksum, @@binlog_format, @@binlog_row_image,
                     @@enforce_gtid_consistency, @@gtid_mode, @@log_bin,  @@sync_binlog,
                     @@log_bin_basename, @@transaction_write_set_extraction,
                     @@lower_case_table_names"""
    result = session.run_sql(stmt)
    object_res = result.fetch_one_object()
    if object_res:
        output = ("\n")
        output += util.output("Replication Information", "")
        for key in object_res:       
            if key == "@@server_id":
                output += util.output("Server Id", "{}".format(object_res[key]),1)
                if advices and object_res[key] == 1:
                    output += util.print_orange("You should consider a unique server id")
                next
            if key == "@@binlog_checksum":
                output += util.output("Binlog Checksum", "{}".format(object_res[key]),1)
                next
            if key == "@@binlog_encryption":
                output += util.output("Binlog Encryption", "{}".format(object_res[key]),1)
                next
            if key == "@@binlog_format":
                output += util.output("Binlog Format", "{}".format(object_res[key]),1)
                if advices and object_res[key] != "ROW":
                    output += util.print_red("You should only use Binary Log format = ROW !")
                next
            if key == "@@binlog_row_image":
                output += util.output("Binlog Row Image", "{}".format(object_res[key]),1)
                next
            if key == "@@binlog_row_metadata":
                output += util.output("Binlog Row Metadata", "{}".format(object_res[key]),1)
                next
            if key == "@@gtid_mode":
                output += util.output("GTID Mode", "{}".format(object_res[key]),1)
                if advices and object_res[key] != "ON":
                    output += util.print_red("You should enable GTIDs !")
                next
            if key == "@@gtid_consistency":
                output += util.output("Enforce GTID Consistency", "{}".format(object_res[key]),1)
                next
            if key == "@@log_bin":
                output += util.output("Log Bin", "{}".format(object_res[key]),1)
                if advices and object_res[key] != 1:
                    output += util.print_red("You should enable binary logs !")
                next
            if key == "@@sync_binlog":
                output += util.output("Sync Binlog", "{}".format(object_res[key]),1)
                next
            if key == "@@log_bin_basename":
                output += util.output("Log Bin Basename", "{}".format(object_res[key]),1)
                next
            if key == "@@transaction_write_set_extraction":
                output += util.output("Trx Write Set Extraction", "{}".format(object_res[key]),1)
                next
            if key == "@@binlog_transaction_dependency_tracking":
                output += util.output("Binlog Trx Dependency Tracking", "{}".format(object_res[key]),1)
                next
            if key == "lower_case_table_names" and object_res[key] == 1:
                output += util.output("Lower Case Table Names", "{}".format(object_res[key]),1)
                if advices:
                    output += util.print_orange("For MDS Inbound Replication, lower_case_table_names should be 0")


    return output
