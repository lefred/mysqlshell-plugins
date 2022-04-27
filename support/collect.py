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

def runPlot(*fns):
  for fn in list(fns)[0]:
    eval(fn)()    

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

    if pyplot_present:
      answer = shell.prompt(
            'Do you want to plot the collected data ? (Y/n) ', {'defaultValue': 'y'})
      if answer.lower() == 'y':
        runPlot(common.plotList)

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
    shell = mysqlsh.globals.shell

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
      common.outdir = operatingsystem.path.dirname(collectpath)
      shutil.unpack_archive(collectpath, common.outdir)
    else:  
      if not operatingsystem.path.exists("{}/metrics.txt".format(collectpath)):
        print("File or path not existing, aborting...")
        return
      common.outdir = collectpath
    
    runPlot(common.plotList)
    
    
