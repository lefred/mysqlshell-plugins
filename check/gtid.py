from mysqlsh.plugin_manager import plugin, plugin_function
import time

def _loop_limit(limit):
   if limit == 0:
      i=0
      while True:
        yield i
   else:
      mylist = range(limit)
      for i in mylist:
        yield i


def _get_executed_gtid_set(session):
    stmt = """select @@gtid_executed;"""
    result = session.run_sql(stmt)
    gtid = result.fetch_one()[0]
    return gtid

@plugin_function("check.getTrxRate")
def get_trx_rate(interval=1, limit=10, session=None):
    """
    Prints amount ox transactions committed in a certain interval.

    This function just returns the amount of transaction processed using GTID executed
    and display it after X interval for Y times.

    Args:
        interval (integer): The optional interval between outputs in seconds (default: 1).
        limit (integer): How many times this should be printed. 0 means infinite loop (default: 10).
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    """
    # Get hold of the global shell object
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return
    str="Amount of transactions committed every {} seconds:".format(interval)
    print(str)
    print("="*len(str))
    previous_gtid_set = _get_executed_gtid_set(session).replace('\n','')
    previous_gtid_tab = previous_gtid_set.split(',')
    for i in _loop_limit(limit):
       time.sleep(interval)
       trx_count = 0
       current_gtid_set = _get_executed_gtid_set(session).replace('\n','')
       current_gtid_tab = current_gtid_set.split(',')
       # here we need to compare the two GTID sets
       for gtid in current_gtid_tab:
          (uuid, seqno)  = gtid.split(':',1)
          #print("   uuid = {}".format(uuid))
          #print("   seqno = {}".format(seqno))
          # check in the previous GTID set if we have that uuid
          matching = [s for s in previous_gtid_tab if uuid in s]
          if matching:
              # it's in it, let's get the seqno
              (uuid_prev, seqno_prev) = matching[0].split(':',1)
              if seqno == seqno_prev:
                  # nothing to do, values are the same
                  next
              else:
                  # seqno are differents
                  # let's get all parts of seqno
                  seqno_sect = seqno.split(':')
                  seqno_prev_sect = seqno_prev.split(':')
                  # let's compare every sections
                  for j in range(len(seqno_sect)):
                      # need to look for the value in the right section
                      str_start_srch = seqno_sect[j].split('-')[0]
                      matching_seq = [s for s in seqno_prev_sect if str_start_srch in s]
                      if matching_seq:
                          # that range was present
                          if matching_seq[0] != seqno_sect[j]:
                              # sections are different
                              # we need to count trx in previous section
                              if "-" in matching_seq[0]:
                                  trx_count_prev = 1 + abs(eval(matching_seq[0]))
                              else:
                                  trx_count_prev = 1
                              # we need to count trx in current section
                              trx_count_curr = 1 + abs(eval(seqno_sect[j]))
                              trx_count += trx_count_curr - trx_count_prev
                      else:
                          # new section
                          # we need to count the trx in this section
                          if "-" in seqno_sect[j]:
                              trx_count += 1 + abs(eval(seqno_sect[j]))
                          else:
                              trx_count += 1
          else:
              seqno_sect = seqno.split(':')
              for j in range(len(seqno_sect)):
                if "-" in seqno_sect[j]:
                    trx_count += 1 + abs(eval(seqno_sect[j]))
                else:
                    trx_count += 1


       previous_gtid_tab = current_gtid_tab
       print("trx/{}sec: {}".format(interval, trx_count))
