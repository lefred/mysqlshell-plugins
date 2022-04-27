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
    import matplotlib.pyplot as plt

    # innodb_log_writes vs innodb_log_write_requests
    data = pd.read_csv("{}/metrics.txt".format(common.outdir), sep='\t')
    innodb_log1 = data[data['Variable_name'] == 'innodb_log_writes']
    innodb_log1 = innodb_log1.astype({'Variable_value':'int'})
    innodb_log1['writes'] = innodb_log1['Variable_value']-innodb_log1['Variable_value'].shift(1)

    innodb_log2 = data[data['Variable_name'] == 'innodb_log_write_requests']
    innodb_log2 = innodb_log2.astype({'Variable_value':'int'})
    innodb_log2['requests'] = innodb_log2['Variable_value']-innodb_log2['Variable_value'].shift(1)

    innodb_log = innodb_log1[['timestamp','writes']].merge(innodb_log2[['timestamp','requests']])
    ax=innodb_log.plot(kind='area',stacked=False, title='InnoDB Log', figsize=(10.24,7.68))
    file_name = "{}/innodb_log.png".format(common.outdir)
    ax.figure.savefig(file_name)
    print("Plot {} generated.".format(file_name))

    # innodb_buffer_pool_reads vs innodb_buffer_pool_read_requests
    innodb_log1 = data[data['Variable_name'] == 'innodb_buffer_pool_reads']
    innodb_log1 = innodb_log1.astype({'Variable_value':'int'})
    innodb_log1['reads'] = innodb_log1['Variable_value']-innodb_log1['Variable_value'].shift(1)

    innodb_log2 = data[data['Variable_name'] == 'innodb_buffer_pool_read_requests']
    innodb_log2 = innodb_log2.astype({'Variable_value':'int'})
    innodb_log2['requests'] = innodb_log2['Variable_value']-innodb_log2['Variable_value'].shift(1)

    innodb_log = innodb_log1[['timestamp','reads']].merge(innodb_log2[['timestamp','requests']])
    ax=innodb_log.plot(kind='area',stacked=False, title='InnoDB Reads', figsize=(10.24,7.68))
    file_name = "{}/innodb_reads.png".format(common.outdir)
    ax.figure.savefig(file_name)
    print("Plot {} generated.".format(file_name))

    # InnoDB Flushing (innodb_buffer_ppol_pages_flushed)
    innodb_log1 = data[data['Variable_name'] == 'innodb_buffer_pool_pages_flushed']
    innodb_log1 = innodb_log1.astype({'Variable_value':'int'})
    innodb_log1['pages flushed'] = innodb_log1['Variable_value']-innodb_log1['Variable_value'].shift(1)

    innodb_log = innodb_log1[['timestamp','pages flushed']]
    ax=innodb_log.plot(kind='area',stacked=False, title='InnoDB Flushing', figsize=(10.24,7.68))
    file_name = "{}/innodb_flushing.png".format(common.outdir)
    ax.figure.savefig(file_name)
    print("Plot {} generated.".format(file_name))

    # InnoDB OS Log Writes (innodb_os_log_written)
    innodb_log1 = data[data['Variable_name'] == 'innodb_os_log_written']
    innodb_log1 = innodb_log1.astype({'Variable_value':'int'})
    innodb_log1['log written'] = innodb_log1['Variable_value']-innodb_log1['Variable_value'].shift(1)

    innodb_log = innodb_log1[['timestamp','log written']]
    ax=innodb_log.plot(kind='area',stacked=False, title='InnoDB OS Log Writes', figsize=(10.24,7.68))
    file_name = "{}/innodb_os_log.png".format(common.outdir)
    ax.figure.savefig(file_name)
    print("Plot {} generated.".format(file_name))   

    # InnoDB Pending
    innodb_log1 = data[data['Variable_name'] == 'innodb_data_pending_fsyncs']
    innodb_log1 = innodb_log1.astype({'Variable_value':'int'})
    innodb_log1['pending_fsyncs'] = innodb_log1['Variable_value']

    innodb_log2 = data[data['Variable_name'] == 'innodb_data_pending_reads']
    innodb_log2 = innodb_log2.astype({'Variable_value':'int'})
    innodb_log2['pending_reads'] = innodb_log2['Variable_value']

    innodb_log3 = data[data['Variable_name'] == 'innodb_data_pending_writes']
    innodb_log3 = innodb_log2.astype({'Variable_value':'int'})
    innodb_log3['pending_writes'] = innodb_log2['Variable_value']

    innodb_log = innodb_log1[['timestamp','pending_fsyncs']].merge(innodb_log2[['timestamp','pending_reads']])
    innodb_log = innodb_log.merge(innodb_log3[['timestamp','pending_writes']])
    ax=innodb_log.plot(kind='area',stacked=False, title='InnoDB Pending', figsize=(10.24,7.68))
    file_name = "{}/innodb_pending.png".format(common.outdir)
    ax.figure.savefig(file_name)
    print("Plot {} generated.".format(file_name)) 

    # Threads Runnings
    

    return