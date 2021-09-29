from support.sections import util


def get_replication_info(session, advices):
    supported = False
    stmt = """select @@server_id, @@binlog_checksum, @@binlog_encryption, @@binlog_format, @@binlog_row_image,
                     @@binlog_row_metadata, @@enforce_gtid_consistency, @@gtid_mode, @@log_bin, 
                     @@log_bin_basename, @@transaction_write_set_extraction,
                     @@binlog_transaction_dependency_tracking"""
    result = session.run_sql(stmt)
    row = result.fetch_one()
    output = ("\n")
    output += util.output("Replication Information", "")
    output += util.output("Server Id", "{}".format(row[0]))
    if advices and row[0] == 1:
        output += util.print_orange("You should consider a unique server id")
    output += util.output("Binlog Checksum", "{}".format(row[1]))
    output += util.output("Binlog Encryption", "{}".format(row[2]))
    output += util.output("Binlog Format", "{}".format(row[3]))
    if advices and row[3] != "ROW":
        output += util.print_red("You should only use Binary Log format = ROW !")
    output += util.output("Binlog Row Image", "{}".format(row[4]))
    output += util.output("Binlog Row Metadata", "{}".format(row[5]))
    output += util.output("GTID Mode", "{}".format(row[7]))
    if advices and row[7] != "ON":
        output += util.print_red("You should enable GTIDs !")
    output += util.output("Enforce GTID Consistency", "{}".format(row[6]))
    output += util.output("Log Bin", "{}".format(row[8]))
    if advices and row[8] != 1:
        output += util.print_red("You should enable binary logs !")
    output += util.output("Log Bin Basename", "{}".format(row[9]))
    output += util.output("Trx Write Set Extraction", "{}".format(row[10]))
    output += util.output("Binlog Trx Dependency Tracking", "{}".format(row[11]))

    return output

