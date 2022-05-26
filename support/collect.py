# support/fetch.py
# -----------------
# Definition of member functions for the support extension object to fetch info
# that can be useful when seeking for help
#

import os as operatingsystem
import sys
import time as mod_time

from mysqlsh.plugin_manager import plugin, plugin_function
from support import fetch
from support.collections import *

global pyplot_present
pyplot_present=False

metrics_modules_matches = {
  "adaptive_hash_index": "module_adaptive_hash",
  "buffer": "module_buffer",
  "buffer_page_io": "module_buffer_page",
  "compression": "module_compress",
  "ddl": "module_ddl",
  "dml": "module_dml",
  "file_system": "module_file",
  "change_buffer": "module_ibuf_system",
  "icp": "module_icp",
  "index": "module_index",
  "innodb": "module_innodb",
  "lock": "module_lock",
  "recovery": "module_log",
  "metadata": "module_metadata",
  "os": "module_os",
  "purge": "module_purge",
  "transaction":  "module_trx"
}

try:
  import shutil
  zip_present=True
except:
  print("Module shutil is not present, final compression won't be available.")
  zip_present=False

try:
  import pandas as pd
  panda_present=True
except:
  print("Module pandas is not present, collected data won't be parsed.")
  panda_present=False
   
if panda_present:
  try:
    import matplotlib.pyplot as plt
    pyplot_present=True
  except:
    print("Module matplotlib is not present, collected data won't be plotted.")
    pyplot_present=False

def _is_mds(session):
    stmt = "select @@version_comment"
    result = session.run_sql(stmt)
    row = result.fetch_one()
    if row[0] == "MySQL Enterprise - Cloud":
      return True
    return False

def _get_all_innodb_metrics_disabled(session):
    # find the disabled susbsytems that don't have a single metric enabled
    stmt = """
           select distinct subsystem, status from INFORMATION_SCHEMA.INNODB_METRICS 
                  where subsystem not in (
                      select subsystem from (
                        select subsystem, count(*) as tot from (
                          select subsystem from INFORMATION_SCHEMA.INNODB_METRICS 
                                 group by subsystem, status
                        ) a  
                        group by 1
                      ) b where b.tot > 1)
           """
    result = session.run_sql(stmt)
    subsystem_disabled=[]
    rows = result.fetch_all()
    for row in rows:
      if row[0] in metrics_modules_matches:
        subsystem_disabled.append(metrics_modules_matches[row[0]])
      else:
        subsystem_disabled.append("module_{}".format(row[0]))

    # find all metrics disabled for a subsytem that also has some metrics enabled
    stmt = """
           select name from INFORMATION_SCHEMA.INNODB_METRICS
                  where subsystem in (
                    select subsystem from (
                      select subsystem, count(*) as tot from (
                        select subsystem from INFORMATION_SCHEMA.INNODB_METRICS 
                               group by subsystem, status
                      ) a  
                      group by 1
                    ) b where b.tot > 1
                  ) and status='disabled'
           """
    result = session.run_sql(stmt)
    metrics_disabled = result.fetch_all()
    return (subsystem_disabled, metrics_disabled)


def _is_module_log_enabled(session):
    stmt = "SELECT count(*) FROM INFORMATION_SCHEMA.INNODB_METRICS WHERE STATUS='disabled'"
    result = session.run_sql(stmt)
    row = result.fetch_one()
    if int(row[0]) == 0:
      return True
    return False
   
def _enable_module_log(session):
     stmt = "SET GLOBAL innodb_monitor_enable = all"
     result = session.run_sql(stmt)
     return

def _disable_module_log(session, rows_subs, rows_metrics):
     # put it back like it was 
     for row in rows_subs:
       stmt = "SET GLOBAL innodb_monitor_disable = {}".format(row)
       result = session.run_sql(stmt)
     for row in rows_metrics:
       stmt = "SET GLOBAL innodb_monitor_disable = {}".format(row[0])
       try:
         result = session.run_sql(stmt)
       except:
         # do nothing
         result=""
     return

def runCollection(session, time, *fns):
  # loop
  minute_cpt = 1
  t_end = mod_time.time() + 60 * time
  starttime = mod_time.time()
  header = False
  while mod_time.time() < t_end:
    for fn in list(fns)[0]:
       eval(fn)(session, header, minute_cpt)

    mod_time.sleep(1.0 - ((mod_time.time() - starttime) % 1.0))
    header = True
    minute_cpt += 1

def runPlot(session, *fns):
  for fn in list(fns)[0]:
    eval(fn)(session)    

@plugin_function("support.collect")
def get_collect_info(mysql=True, os=False, time=10, outputdir="~", session=None):
    """
    Collect data for analysis 

    Args:
        mysql (bool): Fetch data from MySQL (enabled by default).
        os (bool): Fetch data from Operating System (requires to have the Shell running locally) This is disabled by default.
        time (integer): Time in minute to run the collection. Default is 10 minutes.
        outputdir (string): Output directory to store the data. Default is user's home directory.
        session (object): The session to be used on the operation.
    """

    # Get hold of the global shell object
    import mysqlsh
    shell = mysqlsh.globals.shell

    from datetime import datetime

    output = ""
    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    hostname = fetch._get_hostname(session)
    module_log_enabled = _is_module_log_enabled(session)
    module_log_to_disable = False
    if not module_log_enabled:
      answer = shell.prompt(
            'Do you want to enable ALL InnoDB Metrics for logging during the collection ? (Y/n) ', {'defaultValue': 'y'})
      if answer.lower() == 'y':
        all_disabled_subsystem_metrics, all_disabled_metrics  = _get_all_innodb_metrics_disabled(session)
        _enable_module_log(session)
        module_log_to_disable = True
    
    common.outdir = "{}/collect_{}_{}".format(outputdir, hostname, datetime.strftime(datetime.now(), "%Y-%m-%d_%H-%M-%S"))
    common.outdir = operatingsystem.path.expanduser(common.outdir)
    print("Data will be collected in {} for {} minutes".format(common.outdir, time))
    operatingsystem.makedirs(common.outdir)
    ## Open all files for collections
    if _is_mds(session):
        # do collect for MDS specific too
        print("We will also collect MDS specific information.")
        runCollection(session, time, common.collectMDSList)
    
    runCollection(session, time, common.collectList)
    if module_log_to_disable:
      _disable_module_log(session, all_disabled_subsystem_metrics, all_disabled_metrics)
    if pyplot_present:
      answer = shell.prompt(
            'Do you want to plot the collected data ? (Y/n) ', {'defaultValue': 'y'})
      if answer.lower() == 'y':
        runPlot(session, common.plotList)

    if zip_present:
      answer = shell.prompt(
            'Do you want to compress the collected data ? (Y/n) ', {'defaultValue': 'y'})
      if answer.lower() == 'y':
        shutil.make_archive(common.outdir, "zip", common.outdir)
        shutil.rmtree(common.outdir) 

@plugin_function("support.plot")
def plot_collect_info():
    """
    Plot the collected data for analysis 

    """ 
    # Get hold of the global shell object
    import mysqlsh
    global pyplot_present
    shell = mysqlsh.globals.shell
   
    session = shell.get_session()
    if session is None:
        print("No session specified. Either pass a session object to this "
              "function or connect the shell to a database")
        return
    
    if not pyplot_present:
      print("Required modules are missing.")
      return
    collectpath = shell.prompt(
            'Please add the path to the collected data: ')
    collectpath = operatingsystem.path.expanduser(collectpath)
    if collectpath.endswith('.zip'):
      print("Collected data seems to be compressed...")
      if not operatingsystem.path.exists(collectpath):
        print("File doesn't exist, aborting...")
        return
      common.outdir = operatingsystem.path.splitext(collectpath)[0]
      shutil.unpack_archive(collectpath, common.outdir)
    else:  
      if not operatingsystem.path.exists("{}/metrics.txt".format(collectpath)):
        print("File or path not existing, aborting...")
        return
      common.outdir = collectpath
    
    runPlot(session, common.plotList)
    
    
