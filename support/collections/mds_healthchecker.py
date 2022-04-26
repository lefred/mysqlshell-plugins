import support.collections.common as common
common.collectMDSList.append("mds_healthchecker.collect")

def collect(session, header, minute_cpt):
    if (minute_cpt == 1) or (minute_cpt % 60 == 0):
        stmt = """select timestamp, device, total_bytes, available_bytes, 
                         use_percent, mount_point 
                  from performance_schema.health_block_device t1 order by timestamp desc limit 1"""
        common._run_me(session, stmt, header, "mds_block_device.txt")
        stmt = """select * from performance_schema.health_process_memory order by timestamp desc limit 1"""
        common._run_me(session, stmt, header, "mds_process_memory.txt")
        stmt = """select * from performance_schema.health_system_memory order by timestamp desc limit 1"""
        common._run_me(session, stmt, header, "mds_system_memory.txt")
    return
