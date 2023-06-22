import support.collections.common as common
common.collectList.append("innodb_redo.collect")
common.plotList.append("innodb_redo.plot")


def collect(session, header, minute_cpt):
    stmt = "select unix_timestamp() as `timestamp`, 'redo_log_file_nb' as 'Variable_name',  count(*) 'Variable_value' from performance_schema.innodb_redo_log_files"
    common._run_me(session, stmt, header, "innodb_redo.txt")
    return

def plot(session):
    import pandas as pd
    import matplotlib.pyplot as plt
    import matplotlib.font_manager as font_manager
    import matplotlib.colors as pltc
    from random import sample

    all_colors = [k for k,v in pltc.cnames.items()]
    all_colors = [x for x in all_colors if not x.startswith('light')]
    
    common._get_collected_info("{}/global_info.txt".format(common.outdir))

    data = pd.read_csv("{}/innodb_redo.txt".format(common.outdir), sep='\t')
    data=data[data.Variable_value != '18446744073709551615']
    major, middle, minor = common._get_version_info(common.collectedInfo['version'])
    if int(major) == 8 and int(minor) >= 30:
        redo_log_data1 = data[data['Variable_name'] == 'redo_log_file_nb']
        redo_log_data1 = redo_log_data1.astype({'Variable_value':'uint64'})
        redo_log_data1['redo_log_file_nb'] = redo_log_data1['Variable_value']
        mylegend = []
        min=redo_log_data1['redo_log_file_nb'].min()
        max=redo_log_data1['redo_log_file_nb'].max()
        mean=redo_log_data1['redo_log_file_nb'].mean()
        mylegend.append("{} min={} max={} avg={}".format("number of redo log active".ljust(35, " "), 
                                                            str(round(min)).ljust(20, " "), 
                                                            str(round(max)).ljust(20, " "), round(mean)))
        ax=redo_log_data1[["redo_log_file_nb"]].plot(kind="area",stacked=False, 
                        title="MySQL InnoDB Active Redo Log Files", figsize=(10.24,7.68), legend=False)
        # add max amout of files (32)
        mylegend.append("{} = {}".format("max file numbers".ljust(35, " "), 32))
        mycolor = sample(all_colors,1)[0]
        ax.axhline(32,-.5,1, label="max file numbers", c=mycolor)
        h,l = ax.get_legend_handles_labels()
        font = font_manager.FontProperties(family='FantasqueSansMono-Regular.ttf+Powerline+Awesome',
                                    style='normal', size=11)
        ax.legend(h, mylegend, loc='upper center',bbox_to_anchor=(0.5, -0.05), shadow=True, ncol=1, prop=font)
        file_name = "{}/{}".format(common.outdir, "mysql_redo_log.png")
        ax.figure.savefig(file_name)
        print("Plot {} generated.".format(file_name))

    return
    
