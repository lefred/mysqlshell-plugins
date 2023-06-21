from mysqlsh.plugin_manager import plugin, plugin_function
import mysqlsh
try:
  import pyclamd
  pyclam_present = True
except:
  mysqlsh.globals.shell.log("WARNING", 
  "Python module pyClamd is not present, 'scan' plugin won't be usable.")  
  mysqlsh.globals.shell.log("WARNING", 
  "Try:\n   pip install --user pyclamd")  
  pyclam_present = False

@plugin
class scan:
    """
    Scan data for viruses

    Object to connect to ClamAV and scan
    data for known viruses
    """

@plugin_function("scan.version")
def version():
    """
    Displays the version of ClamAV
    """
    if not pyclam_present:
        print("Python module pyClamd is not present, 'scan' plugin won't be usable.")  
        print("Try:\n   pip install --user pyclamd")  
        return
    try:
        cd = pyclamd.ClamdNetworkSocket()    
        cd.ping()
    except pyclamd.ConnectionError:
        print("Could not connect locally to clamd !")
        return
    print(cd.version())
    

@plugin_function("scan.table")
def table(table=None, session=None):
    """
    Scan a full table for viruses

    Scan a full table performing full table scan and check
    for known virues using ClamAV

    Args:
        table (string): The name of the table to be scanned
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    """
    import mysqlsh
    shell = mysqlsh.globals.shell
    if not pyclam_present:
        print("Python module pyClamd is not present, 'scan' plugin won't be usable.")  
        print("Try:\n   pip install --user pyclamd")  
        return
    

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return
    if table is None:
            print("No table name was specified.")
            return

    try:
        cd = pyclamd.ClamdNetworkSocket()    
        cd.ping()
    except pyclamd.ConnectionError:
        print("ERROR: impossible to connect to clamd!")
        return
    stmt = "SELECT * FROM {}".format(table)
    result = session.run_sql(stmt)
    record = result.fetch_one()
    while record:
        to_scan = ','.join(str(x) for x in record)
        out = cd.scan_stream(str.encode(to_scan))
        if out:
            status, virusname = out['stream']
            print("VIRUS FOUND in {} : {} !!".format(table, virusname))
            return
        record = result.fetch_one()
    print("No known virus found in {}".format(table)) 

